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
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.events.ViewChangeEvent;
import com.google.common.base.Preconditions;
import java.util.Map;

public class ViewRefresh extends SingleElementSchemaRefresh<CqlIdentifier, ViewMetadata> {

  public static ViewRefresh dropped(
      DefaultMetadata current, String keyspaceName, String droppedViewName, String logPrefix) {
    return new ViewRefresh(
        current,
        SchemaChangeType.DROPPED,
        null,
        CqlIdentifier.fromInternal(keyspaceName),
        CqlIdentifier.fromInternal(droppedViewName),
        logPrefix);
  }

  public static ViewRefresh createdOrUpdated(
      DefaultMetadata current,
      SchemaChangeType changeType,
      ViewMetadata newView,
      String logPrefix) {
    Preconditions.checkArgument(changeType != SchemaChangeType.DROPPED);
    return (newView == null)
        ? null
        : new ViewRefresh(current, changeType, newView, null, null, logPrefix);
  }

  private ViewRefresh(
      DefaultMetadata current,
      SchemaChangeType changeType,
      ViewMetadata newView,
      CqlIdentifier droppedViewKeyspace,
      CqlIdentifier droppedViewId,
      String logPrefix) {
    super(current, changeType, "view", newView, droppedViewKeyspace, droppedViewId, logPrefix);
  }

  @Override
  protected CqlIdentifier extractKeyspace(ViewMetadata view) {
    return view.getKeyspace();
  }

  @Override
  protected CqlIdentifier extractKey(ViewMetadata view) {
    return view.getName();
  }

  @Override
  protected Map<CqlIdentifier, ViewMetadata> extractElements(KeyspaceMetadata keyspace) {
    return keyspace.getViews();
  }

  @Override
  protected KeyspaceMetadata replace(
      KeyspaceMetadata keyspace, Map<CqlIdentifier, ViewMetadata> newViews) {
    return new DefaultKeyspaceMetadata(
        keyspace.getName(),
        keyspace.isDurableWrites(),
        keyspace.getReplication(),
        keyspace.getUserDefinedTypes(),
        keyspace.getTables(),
        newViews,
        keyspace.getFunctions(),
        keyspace.getAggregates());
  }

  @Override
  protected Object newDroppedEvent(ViewMetadata oldView) {
    return ViewChangeEvent.dropped(oldView);
  }

  @Override
  protected Object newCreatedEvent(ViewMetadata newView) {
    return ViewChangeEvent.created(newView);
  }

  @Override
  protected Object newUpdatedEvent(ViewMetadata oldView, ViewMetadata newView) {
    return ViewChangeEvent.updated(oldView, newView);
  }
}
