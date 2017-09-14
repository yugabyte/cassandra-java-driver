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
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaRefreshRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.events.KeyspaceChangeEvent;
import com.datastax.oss.driver.internal.core.util.ImmutableMaps;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleKeyspaceRefresh extends SchemaRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(SingleKeyspaceRefresh.class);

  @VisibleForTesting public final KeyspaceMetadata newKeyspace;

  public SingleKeyspaceRefresh(
      DefaultMetadata current,
      SchemaRefreshRequest request,
      KeyspaceMetadata newKeyspace,
      String logPrefix) {
    super(current, request, logPrefix);
    this.newKeyspace = newKeyspace;
  }

  @Override
  public void compute() {
    Map<CqlIdentifier, KeyspaceMetadata> oldKeyspaces = oldMetadata.getKeyspaces();

    if (request.type == SchemaChangeType.DROPPED) {
      CqlIdentifier droppedKeyspaceId = CqlIdentifier.fromInternal(request.keyspace);
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
