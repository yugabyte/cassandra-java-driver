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
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaRefreshRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TypeChangeEvent;
import java.util.Map;

public class TypeRefresh extends SingleElementSchemaRefresh<CqlIdentifier, UserDefinedType> {

  public TypeRefresh(
      DefaultMetadata current,
      SchemaRefreshRequest request,
      UserDefinedType newType,
      String logPrefix) {
    super(current, request, newType, logPrefix);
  }

  @Override
  protected CqlIdentifier extractKey(UserDefinedType type) {
    return type.getName();
  }

  @Override
  protected UserDefinedType findElementToDrop(Map<CqlIdentifier, UserDefinedType> oldTypes) {
    return oldTypes.get(CqlIdentifier.fromInternal(request.object));
  }

  @Override
  protected Map<CqlIdentifier, UserDefinedType> extractElements(KeyspaceMetadata keyspace) {
    return keyspace.getUserDefinedTypes();
  }

  @Override
  protected KeyspaceMetadata replace(
      KeyspaceMetadata keyspace, Map<CqlIdentifier, UserDefinedType> newTypes) {
    return new DefaultKeyspaceMetadata(
        keyspace.getName(),
        keyspace.isDurableWrites(),
        keyspace.getReplication(),
        newTypes,
        keyspace.getTables(),
        keyspace.getViews(),
        keyspace.getFunctions(),
        keyspace.getAggregates());
  }

  @Override
  protected Object newDroppedEvent(UserDefinedType oldType) {
    return TypeChangeEvent.dropped(oldType);
  }

  @Override
  protected Object newCreatedEvent(UserDefinedType newType) {
    return TypeChangeEvent.created(newType);
  }

  @Override
  protected Object newUpdatedEvent(UserDefinedType oldType, UserDefinedType newType) {
    return TypeChangeEvent.updated(oldType, newType);
  }
}
