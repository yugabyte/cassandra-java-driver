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
package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import java.util.Optional;

class Cassandra2SchemaQueries extends SchemaQueries {
  Cassandra2SchemaQueries(
      DriverChannel channel, Node node, DriverConfigProfile config, String logPrefix) {
    super(channel, node, config, logPrefix);
  }

  @Override
  protected String selectKeyspacesQuery() {
    return "SELECT * FROM system.schema_keyspaces";
  }

  @Override
  protected String selectTablesQuery() {
    return "SELECT * FROM system.schema_columnfamilies";
  }

  @Override
  protected Optional<String> selectViewsQuery() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> selectIndexesQuery() {
    return Optional.empty();
  }

  @Override
  protected String selectColumnsQuery() {
    return "SELECT * FROM system.schema_columns";
  }

  @Override
  protected String selectTypesQuery() {
    return "SELECT * FROM system.schema_usertypes";
  }

  @Override
  protected String selectFunctionsQuery() {
    return "SELECT * FROM system.schema_functions";
  }

  @Override
  protected String selectAggregatesQuery() {
    return "SELECT * FROM system.schema_aggregates";
  }

  @Override
  protected String tableNameColumn() {
    return "columnfamily_name";
  }

  @Override
  protected String signatureColumn() {
    return "signature";
  }
}
