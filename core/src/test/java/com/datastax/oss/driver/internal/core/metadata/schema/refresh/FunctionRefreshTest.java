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
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultFunctionMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeScope;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaRefreshRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.events.FunctionChangeEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.junit.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

/**
 * Note: this only covers drops, the rest is handled with common code that is already covered in
 * {@link TypeRefreshTest}. Aggregates are also not tested separately since they use the same code
 * for drops.
 */
public class FunctionRefreshTest {

  private static final FunctionMetadata OLD_FOO1 = newFunction("ks", "foo", DataTypes.INT);
  private static final FunctionMetadata OLD_FOO2T =
      newFunction("ks", "foo", DataTypes.INT, DataTypes.TEXT);
  private static final FunctionMetadata OLD_FOO2 =
      newFunction("ks", "foo", DataTypes.INT, DataTypes.INT);
  private static final FunctionMetadata OLD_FOO3 =
      newFunction("ks", "foo", DataTypes.INT, DataTypes.INT, DataTypes.INT);
  private static final DefaultKeyspaceMetadata OLD_KS =
      newKeyspace("ks", OLD_FOO1, OLD_FOO2T, OLD_FOO2, OLD_FOO3);

  private static final DefaultMetadata OLD_METADATA =
      DefaultMetadata.EMPTY.withKeyspaces(ImmutableMap.of(OLD_KS.getName(), OLD_KS));

  @Test
  public void should_remove_dropped_function() {
    FunctionRefresh refresh =
        new FunctionRefresh(
            OLD_METADATA,
            new SchemaRefreshRequest(
                SchemaChangeType.DROPPED,
                SchemaChangeScope.FUNCTION,
                "ks",
                "foo",
                ImmutableList.of("int", "int")),
            null,
            "test");
    refresh.compute();

    KeyspaceMetadata refreshedKs = refresh.newMetadata.getKeyspaces().get(OLD_KS.getName());
    assertThat(refreshedKs.getFunctions().values()).containsOnly(OLD_FOO1, OLD_FOO2T, OLD_FOO3);
    assertThat(refresh.events).containsExactly(FunctionChangeEvent.dropped(OLD_FOO2));
  }

  @Test
  public void should_ignore_dropped_function_if_parameter_types_do_not_match() {
    FunctionRefresh refresh =
        new FunctionRefresh(
            OLD_METADATA,
            new SchemaRefreshRequest(
                SchemaChangeType.DROPPED,
                SchemaChangeScope.FUNCTION,
                "ks",
                "foo",
                ImmutableList.of("text")),
            null,
            "test");
    refresh.compute();

    KeyspaceMetadata refreshedKs = refresh.newMetadata.getKeyspaces().get(OLD_KS.getName());
    assertThat(refreshedKs.getFunctions()).hasSize(4);
    assertThat(refresh.events).isEmpty();
  }

  private static DefaultKeyspaceMetadata newKeyspace(String name, FunctionMetadata... functions) {
    ImmutableMap.Builder<FunctionSignature, FunctionMetadata> functionsBuilder =
        ImmutableMap.builder();
    for (FunctionMetadata function : functions) {
      functionsBuilder.put(function.getSignature(), function);
    }
    return new DefaultKeyspaceMetadata(
        CqlIdentifier.fromInternal(name),
        true,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        functionsBuilder.build(),
        Collections.emptyMap());
  }

  private static FunctionMetadata newFunction(
      String keyspace, String name, DataType... parameterTypes) {
    return new DefaultFunctionMetadata(
        CqlIdentifier.fromInternal(keyspace),
        new FunctionSignature(CqlIdentifier.fromInternal(name), parameterTypes),
        Collections.nCopies(parameterTypes.length, CqlIdentifier.fromInternal("mockParameterName")),
        "mock body",
        true,
        "java",
        DataTypes.INT);
  }
}
