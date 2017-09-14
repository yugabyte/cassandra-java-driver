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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeScope;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaRefreshRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.TypeRefresh;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaParserTypeTest extends SchemaParserTestBase {

  private static final AdminRow PERSON_ROW_2_2 =
      mockTypeRow(
          "ks",
          "person",
          ImmutableList.of("first_name", "last_name", "address"),
          ImmutableList.of(
              "org.apache.cassandra.db.marshal.UTF8Type",
              "org.apache.cassandra.db.marshal.UTF8Type",
              "org.apache.cassandra.db.marshal.UserType("
                  + "ks,61646472657373," // address
                  + "737472656574:org.apache.cassandra.db.marshal.UTF8Type," // street
                  + "7a6970636f6465:org.apache.cassandra.db.marshal.Int32Type)")); // zipcode

  private static final AdminRow PERSON_ROW_3_0 =
      mockTypeRow(
          "ks",
          "person",
          ImmutableList.of("first_name", "last_name", "address"),
          ImmutableList.of("text", "text", "address"));
  public static final SchemaRefreshRequest REQUEST =
      new SchemaRefreshRequest(
          SchemaChangeType.UPDATED, SchemaChangeScope.TYPE, "ks", "person", null);

  @Test
  public void should_skip_when_no_rows() {
    assertThat(parse(REQUEST /*no rows*/)).isNull();
  }

  @Test
  public void should_skip_when_keyspace_unknown() {
    assertThat(parse(REQUEST, PERSON_ROW_3_0)).isNull();
  }

  @Test
  public void should_parse_modern_table() {
    // This test class covers refreshes of a single type. In that scenario, the other types in the
    // keyspace are always known in advance.
    KeyspaceMetadata ks = Mockito.mock(KeyspaceMetadata.class);
    UserDefinedType addressType = Mockito.mock(UserDefinedType.class);
    Mockito.when(addressType.copy(false)).thenReturn(addressType);
    Mockito.when(ks.getUserDefinedTypes())
        .thenReturn(ImmutableMap.of(CqlIdentifier.fromInternal("address"), addressType));
    Mockito.when(currentMetadata.getKeyspaces())
        .thenReturn(ImmutableMap.of(CqlIdentifier.fromInternal("ks"), ks));

    TypeRefresh refresh = (TypeRefresh) parse(REQUEST, PERSON_ROW_3_0);
    assertThat(refresh.request).isEqualTo(REQUEST);

    UserDefinedType type = refresh.newElement;
    assertThat(type.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(type.getName().asInternal()).isEqualTo("person");
    assertThat(type.getFieldNames())
        .containsExactly(
            CqlIdentifier.fromInternal("first_name"),
            CqlIdentifier.fromInternal("last_name"),
            CqlIdentifier.fromInternal("address"));
    assertThat(type.getFieldTypes().get(0)).isEqualTo(DataTypes.TEXT);
    assertThat(type.getFieldTypes().get(1)).isEqualTo(DataTypes.TEXT);
    assertThat(type.getFieldTypes().get(2)).isSameAs(addressType);
  }

  @Test
  public void should_parse_legacy_table() {
    Mockito.when(node.getCassandraVersion()).thenReturn(CassandraVersion.V2_2_0);

    KeyspaceMetadata ks = Mockito.mock(KeyspaceMetadata.class);
    UserDefinedType addressType = Mockito.mock(UserDefinedType.class);
    Mockito.when(ks.getUserDefinedTypes())
        .thenReturn(ImmutableMap.of(CqlIdentifier.fromInternal("address"), addressType));
    Mockito.when(currentMetadata.getKeyspaces())
        .thenReturn(ImmutableMap.of(CqlIdentifier.fromInternal("ks"), ks));

    TypeRefresh refresh = (TypeRefresh) parse(REQUEST, PERSON_ROW_2_2);
    assertThat(refresh.request).isEqualTo(REQUEST);

    UserDefinedType type = refresh.newElement;
    assertThat(type.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(type.getName().asInternal()).isEqualTo("person");
    assertThat(type.getFieldNames())
        .containsExactly(
            CqlIdentifier.fromInternal("first_name"),
            CqlIdentifier.fromInternal("last_name"),
            CqlIdentifier.fromInternal("address"));
    assertThat(type.getFieldTypes().get(0)).isEqualTo(DataTypes.TEXT);
    assertThat(type.getFieldTypes().get(1)).isEqualTo(DataTypes.TEXT);
    assertThat(type.getFieldTypes().get(2)).isSameAs(addressType);
  }

  private MetadataRefresh parse(SchemaRefreshRequest request, AdminRow... typeRows) {
    SchemaRows rows =
        new SchemaRows.Builder(node, request, "table_name", "test")
            .withTypes(Arrays.asList(typeRows))
            .build();
    return new SchemaParser(rows, currentMetadata, context, "test").parse();
  }
}
