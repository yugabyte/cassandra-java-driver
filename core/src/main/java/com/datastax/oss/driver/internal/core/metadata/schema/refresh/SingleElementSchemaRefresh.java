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
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaRefreshRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeCqlNameParser;
import com.datastax.oss.driver.internal.core.util.ImmutableMaps;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common code for refreshes of a single table, view, type, function or aggregate.
 *
 * @param <K> the type of the element's key in the keyspace metadata (simple identifier or
 *     signature).
 * @param <T> the type of the element.
 */
abstract class SingleElementSchemaRefresh<K, T> extends SchemaRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(SingleElementSchemaRefresh.class);

  // null if the change type is DROPPED
  @VisibleForTesting public final T newElement;

  protected SingleElementSchemaRefresh(
      DefaultMetadata current, SchemaRefreshRequest request, T newElement, String logPrefix) {
    super(current, request, logPrefix);
    this.newElement = newElement;
  }

  @Override
  public void compute() {
    Map<CqlIdentifier, KeyspaceMetadata> oldKeyspaces = oldMetadata.getKeyspaces();
    KeyspaceMetadata oldKeyspace = oldKeyspaces.get(CqlIdentifier.fromInternal(request.keyspace));
    if (oldKeyspace == null) {
      LOG.warn(
          "[{}] Got a {} {} event for {}.{}, "
              + "but this keyspace is unknown in our metadata, ignoring",
          logPrefix,
          request.type,
          request.scope,
          request.keyspace,
          request.object);
      newMetadata = oldMetadata;
    } else {
      Map<K, T> oldElements = extractElements(oldKeyspace);
      if (request.type == SchemaChangeType.DROPPED) {
        T oldElement = findElementToDrop(oldElements);
        if (oldElement == null) {
          LOG.warn(
              "[{}] Got a {} {} event for {}.{}, "
                  + "but this element is unknown in our metadata, ignoring",
              logPrefix,
              request.type,
              request.scope,
              request.keyspace,
              request.object + (request.arguments == null ? "" : request.arguments));
          newMetadata = oldMetadata;
        } else {
          Map<K, T> newElements =
              ImmutableMap.copyOf(Maps.filterValues(oldElements, v -> !oldElement.equals(v)));
          KeyspaceMetadata newKeyspace = replace(oldKeyspace, newElements);
          newMetadata =
              oldMetadata.withKeyspaces(
                  ImmutableMaps.replace(oldKeyspaces, newKeyspace.getName(), newKeyspace));
          events.add(newDroppedEvent(oldElement));
        }
      } else {
        K elementKey = extractKey(newElement);
        T oldElement = oldElements.get(elementKey);
        Map<K, T> newElements = ImmutableMaps.replace(oldElements, elementKey, newElement);
        KeyspaceMetadata newKeyspace = replace(oldKeyspace, newElements);
        newMetadata =
            oldMetadata.withKeyspaces(
                ImmutableMaps.replace(oldKeyspaces, newKeyspace.getName(), newKeyspace));
        if (oldElement == null) {
          events.add(newCreatedEvent(newElement));
        } else if (!oldElement.equals(newElement)) { // should always be true, but just in case
          events.add(newUpdatedEvent(oldElement, newElement));
        }
      }
    }
  }

  protected abstract K extractKey(T element);

  protected abstract T findElementToDrop(Map<K, T> oldElements);

  protected abstract Map<K, T> extractElements(KeyspaceMetadata keyspace);

  protected abstract KeyspaceMetadata replace(KeyspaceMetadata keyspace, Map<K, T> newElements);

  protected abstract Object newDroppedEvent(T oldElement);

  protected abstract Object newCreatedEvent(T newElement);

  protected abstract Object newUpdatedEvent(T oldElement, T newElement);

  protected static FunctionSignature buildSignature(
      String keyspaceName,
      String name,
      List<String> arguments,
      DefaultMetadata currentMetadata,
      InternalDriverContext context) {

    CqlIdentifier keyspaceId = CqlIdentifier.fromInternal(keyspaceName);
    KeyspaceMetadata keyspace = currentMetadata.getKeyspaces().get(keyspaceId);
    if (keyspace == null) {
      throw new IllegalArgumentException(
          String.format(
              "Received an schema event for %s.%s(%s), but that keyspace does not exist, ignoring",
              keyspaceName, name, arguments));
    }

    // In push events, function argument types are always sent as names, even in Cassandra 2.2
    DataTypeCqlNameParser dataTypeParser = new DataTypeCqlNameParser();

    ImmutableList.Builder<DataType> argumentTypes = ImmutableList.builder();
    for (String argument : arguments) {
      dataTypeParser.parse(argument, keyspaceId, keyspace.getUserDefinedTypes(), context);
    }
    return new FunctionSignature(CqlIdentifier.fromInternal(name), argumentTypes.build());
  }
}
