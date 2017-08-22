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

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;

public class FullSchemaRefresh extends MetadataRefresh {

  @VisibleForTesting public final List<KeyspaceMetadata> schema;

  public FullSchemaRefresh(
      DefaultMetadata current, List<KeyspaceMetadata> schema, String logPrefix) {
    super(current, logPrefix);
    this.schema = schema;
  }

  @Override
  public void compute() {
    throw new UnsupportedOperationException("TODO");
  }
}
