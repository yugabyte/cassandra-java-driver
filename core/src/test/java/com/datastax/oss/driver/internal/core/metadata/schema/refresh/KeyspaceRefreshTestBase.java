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
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;

abstract class KeyspaceRefreshTestBase {
  protected static final UserDefinedType OLD_T1 =
      new UserDefinedTypeBuilder(
              CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("t1"))
          .withField(CqlIdentifier.fromInternal("i"), DataTypes.INT)
          .build();
  protected static final UserDefinedType OLD_T2 =
      new UserDefinedTypeBuilder(
              CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("t2"))
          .withField(CqlIdentifier.fromInternal("i"), DataTypes.INT)
          .build();
  protected static final DefaultKeyspaceMetadata OLD_KS1 = newKeyspace("ks1", true, OLD_T1, OLD_T2);

  protected DefaultMetadata oldMetadata =
      DefaultMetadata.EMPTY.withKeyspaces(ImmutableMap.of(OLD_KS1.getName(), OLD_KS1));

  protected static DefaultKeyspaceMetadata newKeyspace(
      String name, boolean durableWrites, UserDefinedType... userTypes) {
    ImmutableMap.Builder<CqlIdentifier, UserDefinedType> typesMapBuilder = ImmutableMap.builder();
    for (UserDefinedType type : userTypes) {
      typesMapBuilder.put(type.getName(), type);
    }
    return new DefaultKeyspaceMetadata(
        CqlIdentifier.fromInternal(name),
        durableWrites,
        Collections.emptyMap(),
        typesMapBuilder.build(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap());
  }
}
