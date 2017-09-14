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

import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaRefreshRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.events.FunctionChangeEvent;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionRefresh
    extends SingleElementSchemaRefresh<FunctionSignature, FunctionMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(FunctionRefresh.class);

  public FunctionRefresh(
      DefaultMetadata current,
      SchemaRefreshRequest request,
      FunctionMetadata newFunction,
      String logPrefix) {
    super(current, request, newFunction, logPrefix);
  }

  @Override
  protected FunctionSignature extractKey(FunctionMetadata function) {
    return function.getSignature();
  }

  @Override
  protected FunctionMetadata findElementToDrop(
      Map<FunctionSignature, FunctionMetadata> oldElements) {
    // When we process a DROP request we don't have a FunctionSignature, and re-building it is a bit
    // cumbersome because we would need to re-parse all the types.
    // So instead we go the other way and traverse all known functions, trying to find a signature
    // that matches the request. This is linear, but that shouldn't be a problem unless the number
    // of functions is insane.
    if (oldElements.size() > 10000) {
      LOG.warn(
          "It looks like keyspace {} has more than 10,000 functions! This violates some "
              + "assumptions in the metadata refresh code and might affect performance. "
              + "Please clean up your keyspace, or file a driver ticket if you think you "
              + "have a legitimate use case.",
          request.keyspace);
    }
    for (Map.Entry<FunctionSignature, FunctionMetadata> entry : oldElements.entrySet()) {
      FunctionSignature signature = entry.getKey();
      // Cheap checks: name and number of arguments
      if (signature.getName().asInternal().equals(request.object)
          && signature.getParameterTypes().size() == request.arguments.size()) {
        // Now compare the arguments one by one
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
