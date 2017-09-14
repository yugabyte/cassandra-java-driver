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
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeScope;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaRefreshRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TypeChangeEvent;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import org.junit.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

/** Note: we don't cover TABLE and VIEW separately since they use the same common code. */
public class TypeRefreshTest extends SchemaRefreshTestBase {

  @Test
  public void should_remove_dropped_type() {
    TypeRefresh refresh =
        new TypeRefresh(
            oldMetadata,
            new SchemaRefreshRequest(
                SchemaChangeType.DROPPED, SchemaChangeScope.TYPE, "ks1", "t1", null),
            null,
            "test");
    refresh.compute();

    KeyspaceMetadata refreshedKs1 = refresh.newMetadata.getKeyspaces().get(OLD_KS1.getName());
    assertThat(refreshedKs1.getUserDefinedTypes())
        .containsOnlyKeys(CqlIdentifier.fromInternal("t2"));
    assertThat(refresh.events).containsExactly(TypeChangeEvent.dropped(OLD_T1));
  }

  @Test
  public void should_ignore_dropped_type_if_unknown() {
    TypeRefresh refresh =
        new TypeRefresh(
            oldMetadata,
            new SchemaRefreshRequest(
                SchemaChangeType.DROPPED, SchemaChangeScope.TYPE, "ks1", "t3", null),
            null,
            "test");
    refresh.compute();

    assertThat(refresh.newMetadata).isEqualTo(oldMetadata);
    assertThat(refresh.events).isEmpty();
  }

  @Test
  public void should_ignore_dropped_type_if_keyspace_unknown() {
    TypeRefresh refresh =
        new TypeRefresh(
            oldMetadata,
            new SchemaRefreshRequest(
                SchemaChangeType.DROPPED, SchemaChangeScope.TYPE, "ks2", "t1", null),
            null,
            "test");
    refresh.compute();

    assertThat(refresh.newMetadata).isEqualTo(oldMetadata);
    assertThat(refresh.events).isEmpty();
  }

  @Test
  public void should_add_created_type() {
    UserDefinedType t3 =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("t3"))
            .withField(CqlIdentifier.fromInternal("i"), DataTypes.INT)
            .build();
    TypeRefresh refresh =
        new TypeRefresh(
            oldMetadata,
            new SchemaRefreshRequest(
                SchemaChangeType.CREATED, SchemaChangeScope.TYPE, "ks1", "t3", null),
            t3,
            "test");
    refresh.compute();

    KeyspaceMetadata refreshedKs1 = refresh.newMetadata.getKeyspaces().get(OLD_KS1.getName());
    assertThat(refreshedKs1.getUserDefinedTypes())
        .containsOnlyKeys(
            CqlIdentifier.fromInternal("t1"),
            CqlIdentifier.fromInternal("t2"),
            CqlIdentifier.fromInternal("t3"));
    assertThat(refresh.events).containsExactly(TypeChangeEvent.created(t3));
  }

  @Test
  public void should_modify_updated_type() {
    UserDefinedType newT2 =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("t2"))
            .withField(CqlIdentifier.fromInternal("i"), DataTypes.TEXT)
            .build();
    TypeRefresh refresh =
        new TypeRefresh(
            oldMetadata,
            new SchemaRefreshRequest(
                SchemaChangeType.CREATED, SchemaChangeScope.TYPE, "ks1", "t2", null),
            newT2,
            "test");
    refresh.compute();

    KeyspaceMetadata refreshedKs1 = refresh.newMetadata.getKeyspaces().get(OLD_KS1.getName());
    assertThat(refreshedKs1.getUserDefinedTypes().get(OLD_T2.getName())).isEqualTo(newT2);
    assertThat(refresh.events).containsExactly(TypeChangeEvent.updated(OLD_T2, newT2));
  }
}
