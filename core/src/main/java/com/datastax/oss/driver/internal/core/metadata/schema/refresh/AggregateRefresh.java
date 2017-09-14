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

import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaRefreshRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.events.AggregateChangeEvent;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregateRefresh
    extends SingleElementSchemaRefresh<FunctionSignature, AggregateMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(AggregateRefresh.class);

  public AggregateRefresh(
      DefaultMetadata current,
      SchemaRefreshRequest request,
      AggregateMetadata newAggregate,
      String logPrefix) {
    super(current, request, newAggregate, logPrefix);
  }

  @Override
  protected FunctionSignature extractKey(AggregateMetadata aggregate) {
    return aggregate.getSignature();
  }

  @Override
  protected AggregateMetadata findElementToDrop(
      Map<FunctionSignature, AggregateMetadata> oldElements) {
    // See explanations in FunctionRefresh
    if (oldElements.size() > 10000) {
      LOG.warn(
          "It looks like keyspace {} has more than 10,000 aggregates! This violates some "
              + "assumptions in the metadata refresh code and might affect performance. "
              + "Please clean up your keyspace, or file a driver ticket if you think you "
              + "have a legitimate use case.",
          request.keyspace);
    }
    for (Map.Entry<FunctionSignature, AggregateMetadata> entry : oldElements.entrySet()) {
      FunctionSignature signature = entry.getKey();
      if (signature.getName().asInternal().equals(request.object)
          && signature.getParameterTypes().size() == request.arguments.size()) {
        boolean allMatch = true;
        for (int i = 0; allMatch && i < signature.getParameterTypes().size(); i++) {
          DataType type = signature.getParameterTypes().get(i);
          String name = request.arguments.get(i);
          allMatch = (name.equals(type.asCql(false, true)));
        }
        if (allMatch) {
          return entry.getValue();
        }
      }
    }
    return null;
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
