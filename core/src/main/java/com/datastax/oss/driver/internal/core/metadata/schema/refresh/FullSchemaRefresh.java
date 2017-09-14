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
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaRefreshRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.events.KeyspaceChangeEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.util.Map;

public class FullSchemaRefresh extends SchemaRefresh {

  @VisibleForTesting public final Map<CqlIdentifier, KeyspaceMetadata> newKeyspaces;

  public FullSchemaRefresh(
      DefaultMetadata current,
      SchemaRefreshRequest request,
      Map<CqlIdentifier, KeyspaceMetadata> newKeyspaces,
      String logPrefix) {
    super(current, request, logPrefix);
    this.newKeyspaces = newKeyspaces;
  }

  @Override
  public void compute() {
    newMetadata = oldMetadata.withKeyspaces(this.newKeyspaces);

    Map<CqlIdentifier, KeyspaceMetadata> oldKeyspaces = oldMetadata.getKeyspaces();
    for (CqlIdentifier removedKey : Sets.difference(oldKeyspaces.keySet(), newKeyspaces.keySet())) {
      events.add(KeyspaceChangeEvent.dropped(oldKeyspaces.get(removedKey)));
    }
    for (Map.Entry<CqlIdentifier, KeyspaceMetadata> entry : newKeyspaces.entrySet()) {
      CqlIdentifier key = entry.getKey();
      computeEvents(oldKeyspaces.get(key), entry.getValue());
    }
  }
}
