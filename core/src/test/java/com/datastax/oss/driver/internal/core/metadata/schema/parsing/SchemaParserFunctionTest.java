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
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeScope;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.FunctionRefresh;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.junit.Test;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;

public class SchemaParserFunctionTest extends SchemaParserTest {

  private static final AdminRow ID_ROW_2_2 =
      mockFunctionRow(
          "ks",
          "id",
          ImmutableList.of("i"),
          ImmutableList.of("org.apache.cassandra.db.marshal.Int32Type"),
          "return i;",
          false,
          "java",
          "org.apache.cassandra.db.marshal.Int32Type");

  static final AdminRow ID_ROW_3_0 =
      mockFunctionRow(
          "ks",
          "id",
          ImmutableList.of("i"),
          ImmutableList.of("int"),
          "return i;",
          false,
          "java",
          "int");

  @Test
  public void should_skip_when_no_rows() {
    assertThat(parse(/*no rows*/ )).isNull();
  }

  @Test
  public void should_skip_when_keyspace_unknown() {
    assertThat(parse(ID_ROW_3_0)).isNull();
  }

  @Test
  public void should_parse_modern_table() {
    KeyspaceMetadata ks = Mockito.mock(KeyspaceMetadata.class);
    Mockito.when(currentMetadata.getKeyspaces())
        .thenReturn(ImmutableMap.of(CqlIdentifier.fromInternal("ks"), ks));

    FunctionRefresh refresh = (FunctionRefresh) parse(ID_ROW_3_0);
    assertThat(refresh.changeType).isEqualTo(SchemaChangeType.UPDATED);

    FunctionMetadata function = refresh.function;
    assertThat(function.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(function.getSignature().getName().asInternal()).isEqualTo("id");
    assertThat(function.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);
    assertThat(function.getParameterNames()).containsExactly(CqlIdentifier.fromInternal("i"));
    assertThat(function.getBody()).isEqualTo("return i;");
    assertThat(function.isCalledOnNullInput()).isFalse();
    assertThat(function.getLanguage()).isEqualTo("java");
    assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
  }

  @Test
  public void should_parse_legacy_table() {
    Mockito.when(node.getCassandraVersion()).thenReturn(CassandraVersion.V2_2_0);

    KeyspaceMetadata ks = Mockito.mock(KeyspaceMetadata.class);
    Mockito.when(currentMetadata.getKeyspaces())
        .thenReturn(ImmutableMap.of(CqlIdentifier.fromInternal("ks"), ks));

    FunctionRefresh refresh = (FunctionRefresh) parse(ID_ROW_2_2);
    assertThat(refresh.changeType).isEqualTo(SchemaChangeType.UPDATED);

    FunctionMetadata function = refresh.function;
    assertThat(function.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(function.getSignature().getName().asInternal()).isEqualTo("id");
    assertThat(function.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);
    assertThat(function.getParameterNames()).containsExactly(CqlIdentifier.fromInternal("i"));
    assertThat(function.getBody()).isEqualTo("return i;");
    assertThat(function.isCalledOnNullInput()).isFalse();
    assertThat(function.getLanguage()).isEqualTo("java");
    assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
  }

  private MetadataRefresh parse(AdminRow... functionRows) {
    SchemaRows rows =
        new SchemaRows.Builder(
                node, SchemaChangeType.UPDATED, SchemaChangeScope.FUNCTION, "table_name", "test")
            .withFunctions(Arrays.asList(functionRows))
            .build();
    return new SchemaParser(rows, currentMetadata, context, "test").parse();
  }
}
