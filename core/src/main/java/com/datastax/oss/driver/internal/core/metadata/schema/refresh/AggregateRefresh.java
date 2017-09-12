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
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.events.AggregateChangeEvent;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;

public class AggregateRefresh
    extends SingleElementSchemaRefresh<FunctionSignature, AggregateMetadata> {

  public static AggregateRefresh dropped(
      DefaultMetadata current,
      String keyspaceName,
      String droppedAggregateName,
      List<String> droppedAggregateArguments,
      InternalDriverContext context) {
    return new AggregateRefresh(
        current,
        SchemaChangeType.DROPPED,
        null,
        CqlIdentifier.fromInternal(keyspaceName),
        buildSignature(
            keyspaceName, droppedAggregateName, droppedAggregateArguments, current, context),
        context.clusterName());
  }

  public static AggregateRefresh createdOrUpdated(
      DefaultMetadata current,
      SchemaChangeType changeType,
      AggregateMetadata newAggregate,
      String logPrefix) {
    Preconditions.checkArgument(changeType != SchemaChangeType.DROPPED);
    return (newAggregate == null)
        ? null
        : new AggregateRefresh(current, changeType, newAggregate, null, null, logPrefix);
  }

  private AggregateRefresh(
      DefaultMetadata current,
      SchemaChangeType changeType,
      AggregateMetadata newAggregate,
      CqlIdentifier droppedAggregateKeyspace,
      FunctionSignature droppedAggregateId,
      String logPrefix) {
    super(
        current,
        changeType,
        "aggregate",
        newAggregate,
        droppedAggregateKeyspace,
        droppedAggregateId,
        logPrefix);
  }

  @Override
  protected CqlIdentifier extractKeyspace(AggregateMetadata aggregate) {
    return aggregate.getKeyspace();
  }

  @Override
  protected FunctionSignature extractKey(AggregateMetadata aggregate) {
    return aggregate.getSignature();
  }

  @Override
  protected Map<FunctionSignature, AggregateMetadata> extractElements(KeyspaceMetadata keyspace) {
    return keyspace.getAggregates();
  }

  @Override
  protected KeyspaceMetadata replace(
      KeyspaceMetadata keyspace, Map<FunctionSignature, AggregateMetadata> newAggregates) {
    return new DefaultKeyspaceMetadata(
        keyspace.getName(),
        keyspace.isDurableWrites(),
        keyspace.getReplication(),
        keyspace.getUserDefinedTypes(),
        keyspace.getTables(),
        keyspace.getViews(),
        keyspace.getFunctions(),
        newAggregates);
  }

  @Override
  protected Object newDroppedEvent(AggregateMetadata oldAggregate) {
    return AggregateChangeEvent.dropped(oldAggregate);
  }

  @Override
  protected Object newCreatedEvent(AggregateMetadata newAggregate) {
    return AggregateChangeEvent.created(newAggregate);
  }

  @Override
  protected Object newUpdatedEvent(AggregateMetadata oldAggregate, AggregateMetadata newAggregate) {
    return AggregateChangeEvent.updated(oldAggregate, newAggregate);
  }
}
