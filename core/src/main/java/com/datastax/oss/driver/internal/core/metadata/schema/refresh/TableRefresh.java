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
package com.datastax.oss.driver.internal.core.metadata.schema.refresh;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaRefreshRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TableChangeEvent;
import java.util.Map;

public class TableRefresh extends SingleElementSchemaRefresh<CqlIdentifier, TableMetadata> {

  public TableRefresh(
      DefaultMetadata current,
      SchemaRefreshRequest request,
      TableMetadata newTable,
      String logPrefix) {
    super(current, request, newTable, logPrefix);
  }

  @Override
  protected CqlIdentifier extractKey(TableMetadata table) {
    return table.getName();
  }

  @Override
  protected TableMetadata findElementToDrop(Map<CqlIdentifier, TableMetadata> oldTables) {
    return oldTables.get(CqlIdentifier.fromInternal(request.object));
  }

  @Override
  protected Map<CqlIdentifier, TableMetadata> extractElements(KeyspaceMetadata keyspace) {
    return keyspace.getTables();
  }

  @Override
  protected KeyspaceMetadata replace(
      KeyspaceMetadata keyspace, Map<CqlIdentifier, TableMetadata> newTables) {
    return new DefaultKeyspaceMetadata(
        keyspace.getName(),
        keyspace.isDurableWrites(),
        keyspace.getReplication(),
        keyspace.getUserDefinedTypes(),
        newTables,
        keyspace.getViews(),
        keyspace.getFunctions(),
        keyspace.getAggregates());
  }

  @Override
  protected Object newDroppedEvent(TableMetadata oldTable) {
    return TableChangeEvent.dropped(oldTable);
  }

  @Override
  protected Object newCreatedEvent(TableMetadata newTable) {
    return TableChangeEvent.created(newTable);
  }

  @Override
  protected Object newUpdatedEvent(TableMetadata oldTable, TableMetadata newTable) {
    return TableChangeEvent.updated(oldTable, newTable);
  }
}
