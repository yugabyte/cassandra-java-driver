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
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeScope;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaRefreshRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.AggregateRefresh;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;

public class SchemaParserAggregateTest extends SchemaParserTestBase {

  private static final AdminRow SUM_AND_TO_STRING_ROW_2_2 =
      mockAggregateRow(
          "ks",
          "sum_and_to_string",
          ImmutableList.of("org.apache.cassandra.db.marshal.Int32Type"),
          "plus",
          "org.apache.cassandra.db.marshal.Int32Type",
          "to_string",
          "org.apache.cassandra.db.marshal.UTF8Type",
          Bytes.fromHexString("0x00000000"));

  static final AdminRow SUM_AND_TO_STRING_ROW_3_0 =
      mockAggregateRow(
          "ks",
          "sum_and_to_string",
          ImmutableList.of("int"),
          "plus",
          "int",
          "to_string",
          "text",
          "0");

  private static final SchemaRefreshRequest REQUEST =
      new SchemaRefreshRequest(
          SchemaChangeType.UPDATED,
          SchemaChangeScope.AGGREGATE,
          "ks",
          "sum_and_to_string",
          ImmutableList.of("int"));

  @Before
  public void setup() {
    super.setup();
    Mockito.when(context.codecRegistry()).thenReturn(new DefaultCodecRegistry("test"));
    Mockito.when(context.protocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
  }

  @Test
  public void should_skip_when_no_rows() {
    assertThat(parse(REQUEST /*no rows*/)).isNull();
  }

  @Test
  public void should_skip_when_keyspace_unknown() {
    assertThat(parse(REQUEST, SUM_AND_TO_STRING_ROW_3_0)).isNull();
  }

  @Test
  public void should_parse_modern_table() {
    KeyspaceMetadata ks = Mockito.mock(KeyspaceMetadata.class);
    Mockito.when(currentMetadata.getKeyspaces())
        .thenReturn(ImmutableMap.of(CqlIdentifier.fromInternal("ks"), ks));

    AggregateRefresh refresh = (AggregateRefresh) parse(REQUEST, SUM_AND_TO_STRING_ROW_3_0);
    assertThat(refresh.request).isEqualTo(REQUEST);

    AggregateMetadata aggregate = refresh.newElement;
    assertThat(aggregate.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(aggregate.getSignature().getName().asInternal()).isEqualTo("sum_and_to_string");
    assertThat(aggregate.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);

    FunctionSignature stateFuncSignature = aggregate.getStateFuncSignature();
    assertThat(stateFuncSignature.getName().asInternal()).isEqualTo("plus");
    assertThat(stateFuncSignature.getParameterTypes())
        .containsExactly(DataTypes.INT, DataTypes.INT);
    assertThat(aggregate.getStateType()).isEqualTo(DataTypes.INT);

    FunctionSignature finalFuncSignature = aggregate.getFinalFuncSignature();
    assertThat(finalFuncSignature.getName().asInternal()).isEqualTo("to_string");
    assertThat(finalFuncSignature.getParameterTypes()).containsExactly(DataTypes.INT);
    assertThat(aggregate.getReturnType()).isEqualTo(DataTypes.TEXT);

    assertThat(aggregate.getInitCond()).isInstanceOf(Integer.class).isEqualTo(0);
  }

  @Test
  public void should_parse_legacy_table() {
    Mockito.when(node.getCassandraVersion()).thenReturn(CassandraVersion.V2_2_0);

    KeyspaceMetadata ks = Mockito.mock(KeyspaceMetadata.class);
    Mockito.when(currentMetadata.getKeyspaces())
        .thenReturn(ImmutableMap.of(CqlIdentifier.fromInternal("ks"), ks));

    AggregateRefresh refresh = (AggregateRefresh) parse(REQUEST, SUM_AND_TO_STRING_ROW_2_2);
    assertThat(refresh.request).isEqualTo(REQUEST);

    AggregateMetadata aggregate = refresh.newElement;
    assertThat(aggregate.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(aggregate.getSignature().getName().asInternal()).isEqualTo("sum_and_to_string");
    assertThat(aggregate.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);

    FunctionSignature stateFuncSignature = aggregate.getStateFuncSignature();
    assertThat(stateFuncSignature.getName().asInternal()).isEqualTo("plus");
    assertThat(stateFuncSignature.getParameterTypes())
        .containsExactly(DataTypes.INT, DataTypes.INT);
    assertThat(aggregate.getStateType()).isEqualTo(DataTypes.INT);

    FunctionSignature finalFuncSignature = aggregate.getFinalFuncSignature();
    assertThat(finalFuncSignature.getName().asInternal()).isEqualTo("to_string");
    assertThat(finalFuncSignature.getParameterTypes()).containsExactly(DataTypes.INT);
    assertThat(aggregate.getReturnType()).isEqualTo(DataTypes.TEXT);

    assertThat(aggregate.getInitCond()).isInstanceOf(Integer.class).isEqualTo(0);
  }

  private MetadataRefresh parse(SchemaRefreshRequest request, AdminRow... aggregateRows) {
    SchemaRows rows =
        new SchemaRows.Builder(node, request, "table_name", "test")
            .withAggregates(Arrays.asList(aggregateRows))
            .build();
    return new SchemaParser(rows, currentMetadata, context, "test").parse();
  }
}
