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
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.events.FunctionChangeEvent;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;

public class FunctionRefresh
    extends SingleElementSchemaRefresh<FunctionSignature, FunctionMetadata> {

  public static FunctionRefresh dropped(
      DefaultMetadata current,
      String keyspaceName,
      String droppedFunctionName,
      List<String> droppedFunctionArguments,
      InternalDriverContext context) {
    return new FunctionRefresh(
        current,
        SchemaChangeType.DROPPED,
        null,
        CqlIdentifier.fromInternal(keyspaceName),
        buildSignature(
            keyspaceName, droppedFunctionName, droppedFunctionArguments, current, context),
        context.clusterName());
  }

  public static FunctionRefresh createdOrUpdated(
      DefaultMetadata current,
      SchemaChangeType changeType,
      FunctionMetadata newFunction,
      String logPrefix) {
    Preconditions.checkArgument(changeType != SchemaChangeType.DROPPED);
    return (newFunction == null)
        ? null
        : new FunctionRefresh(current, changeType, newFunction, null, null, logPrefix);
  }

  private FunctionRefresh(
      DefaultMetadata current,
      SchemaChangeType changeType,
      FunctionMetadata newFunction,
      CqlIdentifier droppedFunctionKeyspace,
      FunctionSignature droppedFunctionId,
      String logPrefix) {
    super(
        current,
        changeType,
        "function",
        newFunction,
        droppedFunctionKeyspace,
        droppedFunctionId,
        logPrefix);
  }

  @Override
  protected CqlIdentifier extractKeyspace(FunctionMetadata function) {
    return function.getKeyspace();
  }

  @Override
  protected FunctionSignature extractKey(FunctionMetadata function) {
    return function.getSignature();
  }

  @Override
  protected Map<FunctionSignature, FunctionMetadata> extractElements(KeyspaceMetadata keyspace) {
    return keyspace.getFunctions();
  }

  @Override
  protected KeyspaceMetadata replace(
      KeyspaceMetadata keyspace, Map<FunctionSignature, FunctionMetadata> newFunctions) {
    return new DefaultKeyspaceMetadata(
        keyspace.getName(),
        keyspace.isDurableWrites(),
        keyspace.getReplication(),
        keyspace.getUserDefinedTypes(),
        keyspace.getTables(),
        keyspace.getViews(),
        newFunctions,
        keyspace.getAggregates());
  }

  @Override
  protected Object newDroppedEvent(FunctionMetadata oldFunction) {
    return FunctionChangeEvent.dropped(oldFunction);
  }

  @Override
  protected Object newCreatedEvent(FunctionMetadata newFunction) {
    return FunctionChangeEvent.created(newFunction);
  }

  @Override
  protected Object newUpdatedEvent(FunctionMetadata oldFunction, FunctionMetadata newFunction) {
    return FunctionChangeEvent.updated(oldFunction, newFunction);
  }
}
