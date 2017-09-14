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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.SchemaRefresh;
import com.google.common.collect.Multimap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses elements whose definition is contained in a single row (everything except schema, keyspace
 * and table).
 */
abstract class SchemaSingleRowElementParser<T> extends SchemaElementParser {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaSingleRowElementParser.class);

  private final Multimap<CqlIdentifier, AdminRow> elementRows;

  /**
   * @param elementRows the field in parent.rows corresponding to the current scope, for example
   *     parent.rows.functions for a function update.
   */
  SchemaSingleRowElementParser(SchemaParser parent, Multimap<CqlIdentifier, AdminRow> elementRows) {
    super(parent);
    this.elementRows = elementRows;
  }

  /** Called when the whole refresh is a single element of this type. */
  SchemaRefresh parse() {
    if (elementRows.size() != 1) {
      LOG.warn(
          "[{}] Processing {} refresh, expected a single row but found {}, skipping",
          logPrefix,
          rows.request.scope,
          elementRows.size());
      return null;
    } else {
      Map.Entry<CqlIdentifier, AdminRow> entry = elementRows.entries().iterator().next();
      CqlIdentifier keyspaceId = entry.getKey();
      AdminRow row = entry.getValue();
      // Since we're refreshing a single element, we know that any UDT it depends on is already
      // present in the keyspace.
      KeyspaceMetadata keyspace = currentMetadata.getKeyspaces().get(keyspaceId);
      if (keyspace == null) {
        LOG.warn(
            "[{}] Processing {} refresh for {}.{} " + "but that keyspace is unknown, skipping",
            logPrefix,
            rows.request.scope,
            keyspaceId,
            row.getString(rows.request.scope.name().toLowerCase() + "_name"));
        return null;
      }
      T t = parseRow(row, keyspaceId, keyspace.getUserDefinedTypes());
      return (t == null) ? null : newRefresh(t);
    }
  }

  /**
   * Called by {@link #parse()}, or directly if this row is part of a full schema or keyspace
   * refresh.
   */
  abstract T parseRow(
      AdminRow row, CqlIdentifier keyspaceId, Map<CqlIdentifier, UserDefinedType> userDefinedTypes);

  abstract SchemaRefresh newRefresh(T t);
}
