/*
 * Copyright (C) 2017-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.SchemaRefresh;

/**
 * The main entry point for system schema rows parsing.
 *
 * <p>The code for each type of row is split into {@link SchemaElementParser} subclasses; the only
 * reason for that is to keep this class reasonably short. Schema refreshes are not on the hot path
 * so creating a few extra objects shouldn't matter.
 */
public class SchemaParser {

  final SchemaRows rows;
  final DefaultMetadata currentMetadata;
  final InternalDriverContext context;
  final String logPrefix;
  final DataTypeParser dataTypeParser;

  public SchemaParser(
      SchemaRows rows,
      DefaultMetadata currentMetadata,
      InternalDriverContext context,
      String logPrefix) {
    this.rows = rows;
    this.currentMetadata = currentMetadata;
    this.context = context;
    this.logPrefix = logPrefix;

    CassandraVersion version = rows.node.getCassandraVersion().nextStable();
    this.dataTypeParser =
        version.compareTo(CassandraVersion.V3_0_0) < 0
            ? new DataTypeClassNameParser()
            : new DataTypeCqlNameParser();
  }

  /**
   * @return the refresh, or {@code null} if it could not be computed (most likely due to malformed
   *     system rows).
   */
  public SchemaRefresh parse() {
    switch (rows.request.scope) {
      case FULL_SCHEMA:
        return new FullSchemaParser(this).parse();
      case KEYSPACE:
        return new KeyspaceParser(this).parse();
      case TABLE:
        return new TableParser(this).parse();
      case VIEW:
        return new ViewParser(this).parse();
      case TYPE:
        return new UserDefinedTypeParser(this).parse();
      case FUNCTION:
        return new FunctionParser(this).parse();
      case AGGREGATE:
        return new AggregateParser(this).parse();
      default:
        throw new AssertionError("Unsupported schema refresh kind " + rows.request.scope);
    }
  }
}
