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
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.events.KeyspaceChangeEvent;
import com.datastax.oss.driver.internal.core.util.ImmutableMaps;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleKeyspaceRefresh extends KeyspaceRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(SingleKeyspaceRefresh.class);

  public static SingleKeyspaceRefresh dropped(
      DefaultMetadata current, String droppedKeyspaceName, String logPrefix) {
    return new SingleKeyspaceRefresh(
        current,
        SchemaChangeType.DROPPED,
        null,
        CqlIdentifier.fromInternal(droppedKeyspaceName),
        logPrefix);
  }

  public static SingleKeyspaceRefresh createdOrUpdated(
      DefaultMetadata current,
      SchemaChangeType changeType,
      KeyspaceMetadata newKeyspace,
      String logPrefix) {
    Preconditions.checkArgument(changeType != SchemaChangeType.DROPPED);
    return (newKeyspace == null)
        ? null
        : new SingleKeyspaceRefresh(current, changeType, newKeyspace, null, logPrefix);
  }

  @VisibleForTesting public final SchemaChangeType changeType;
  @VisibleForTesting public final KeyspaceMetadata newKeyspace;
  private final CqlIdentifier droppedKeyspaceId;

  private SingleKeyspaceRefresh(
      DefaultMetadata current,
      SchemaChangeType changeType,
      KeyspaceMetadata newKeyspace,
      CqlIdentifier droppedKeyspaceId,
      String logPrefix) {
    super(current, logPrefix);
    this.changeType = changeType;
    this.newKeyspace = newKeyspace;
    this.droppedKeyspaceId = droppedKeyspaceId;
  }

  @Override
  public void compute() {
    Map<CqlIdentifier, KeyspaceMetadata> oldKeyspaces = oldMetadata.getKeyspaces();

    if (changeType == SchemaChangeType.DROPPED) {
      KeyspaceMetadata oldKeyspace = oldKeyspaces.get(droppedKeyspaceId);
      if (oldKeyspace == null) {
        LOG.warn(
            "[{}] Got a DROPPED event for {}, "
                + "but this keyspace is unknown in our metadata, ignoring",
            logPrefix,
            droppedKeyspaceId);
        newMetadata = oldMetadata;
      } else {
        Map<CqlIdentifier, KeyspaceMetadata> newKeyspaces =
            ImmutableMap.copyOf(Maps.filterKeys(oldKeyspaces, k -> !droppedKeyspaceId.equals(k)));
        newMetadata = oldMetadata.withKeyspaces(newKeyspaces);
        events.add(KeyspaceChangeEvent.dropped(oldKeyspace));
      }
    } else {
      Map<CqlIdentifier, KeyspaceMetadata> newKeyspaces =
          ImmutableMaps.replace(oldKeyspaces, newKeyspace.getName(), newKeyspace);
      newMetadata = oldMetadata.withKeyspaces(newKeyspaces);

      KeyspaceMetadata oldKeyspace = oldKeyspaces.get(newKeyspace.getName());
      computeEvents(oldKeyspace, newKeyspace);
    }
  }
}
