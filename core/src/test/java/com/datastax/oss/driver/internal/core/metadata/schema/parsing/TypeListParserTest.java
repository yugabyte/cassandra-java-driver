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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeScope;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class TypeListParserTest extends SchemaParserTest {
  private static final CqlIdentifier KEYSPACE_ID = CqlIdentifier.fromInternal("ks");

  private TypeListParser parser;

  @Before
  public void setup() {
    super.setup();
    SchemaRows rows =
        new SchemaRows.Builder(
                node, SchemaChangeType.UPDATED, SchemaChangeScope.KEYSPACE, "table_name", "test")
            .build();
    SchemaParser parent = new SchemaParser(rows, currentMetadata, context, "test");

    parser = new TypeListParser(parent, KEYSPACE_ID);
  }

  @Test
  public void should_parse_empty_list() {
    assertThat(parser.parse(Collections.emptyList())).isEmpty();
  }

  @Test
  public void should_parse_singleton_list() {
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(mockTypeRow("ks", "t", ImmutableList.of("i"), ImmutableList.of("int")));

    assertThat(types).hasSize(1);
    UserDefinedType type = types.get(CqlIdentifier.fromInternal("t"));
    assertThat(type.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(type.getName().asInternal()).isEqualTo("t");
    assertThat(type.getFieldNames()).containsExactly(CqlIdentifier.fromInternal("i"));
    assertThat(type.getFieldTypes()).containsExactly(DataTypes.INT);
  }

  @Test
  public void should_resolve_direct_dependency() {
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(
            mockTypeRow("ks", "a", ImmutableList.of("b"), ImmutableList.of("b")),
            mockTypeRow("ks", "b", ImmutableList.of("i"), ImmutableList.of("int")));

    assertThat(types).hasSize(2);
    UserDefinedType aType = types.get(CqlIdentifier.fromInternal("a"));
    UserDefinedType bType = types.get(CqlIdentifier.fromInternal("b"));
    assertThat(aType.getFieldTypes().get(0)).isEqualTo(bType);
  }

  @Test
  public void should_resolve_list_dependency() {
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(
            mockTypeRow(
                "ks", "a", ImmutableList.of("bs"), ImmutableList.of("frozen<list<frozen<b>>>")),
            mockTypeRow("ks", "b", ImmutableList.of("i"), ImmutableList.of("int")));

    assertThat(types).hasSize(2);
    UserDefinedType aType = types.get(CqlIdentifier.fromInternal("a"));
    UserDefinedType bType = types.get(CqlIdentifier.fromInternal("b"));
    assertThat(((ListType) aType.getFieldTypes().get(0)).getElementType()).isEqualTo(bType);
  }

  @Test
  public void should_resolve_set_dependency() {
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(
            mockTypeRow(
                "ks", "a", ImmutableList.of("bs"), ImmutableList.of("frozen<set<frozen<b>>>")),
            mockTypeRow("ks", "b", ImmutableList.of("i"), ImmutableList.of("int")));

    assertThat(types).hasSize(2);
    UserDefinedType aType = types.get(CqlIdentifier.fromInternal("a"));
    UserDefinedType bType = types.get(CqlIdentifier.fromInternal("b"));
    assertThat(((SetType) aType.getFieldTypes().get(0)).getElementType()).isEqualTo(bType);
  }

  @Test
  public void should_resolve_map_dependency() {
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(
            mockTypeRow(
                "ks",
                "a1",
                ImmutableList.of("bs"),
                ImmutableList.of("frozen<map<int, frozen<b>>>")),
            mockTypeRow(
                "ks",
                "a2",
                ImmutableList.of("bs"),
                ImmutableList.of("frozen<map<frozen<b>, int>>")),
            mockTypeRow("ks", "b", ImmutableList.of("i"), ImmutableList.of("int")));

    assertThat(types).hasSize(3);
    UserDefinedType a1Type = types.get(CqlIdentifier.fromInternal("a1"));
    UserDefinedType a2Type = types.get(CqlIdentifier.fromInternal("a2"));
    UserDefinedType bType = types.get(CqlIdentifier.fromInternal("b"));
    assertThat(((MapType) a1Type.getFieldTypes().get(0)).getValueType()).isEqualTo(bType);
    assertThat(((MapType) a2Type.getFieldTypes().get(0)).getKeyType()).isEqualTo(bType);
  }

  @Test
  public void should_resolve_tuple_dependency() {
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(
            mockTypeRow(
                "ks",
                "a",
                ImmutableList.of("b"),
                ImmutableList.of("frozen<tuple<int, frozen<b>>>")),
            mockTypeRow("ks", "b", ImmutableList.of("i"), ImmutableList.of("int")));

    assertThat(types).hasSize(2);
    UserDefinedType aType = types.get(CqlIdentifier.fromInternal("a"));
    UserDefinedType bType = types.get(CqlIdentifier.fromInternal("b"));
    assertThat(((TupleType) aType.getFieldTypes().get(0)).getComponentTypes().get(1))
        .isEqualTo(bType);
  }

  @Test
  public void should_resolve_nested_dependency() {
    Map<CqlIdentifier, UserDefinedType> types =
        parser.parse(
            mockTypeRow(
                "ks",
                "a",
                ImmutableList.of("bs"),
                ImmutableList.of("frozen<tuple<int, frozen<list<frozen<b>>>>>")),
            mockTypeRow("ks", "b", ImmutableList.of("i"), ImmutableList.of("int")));

    assertThat(types).hasSize(2);
    UserDefinedType aType = types.get(CqlIdentifier.fromInternal("a"));
    UserDefinedType bType = types.get(CqlIdentifier.fromInternal("b"));
    TupleType tupleType = (TupleType) aType.getFieldTypes().get(0);
    ListType listType = (ListType) tupleType.getComponentTypes().get(1);
    assertThat(listType.getElementType()).isEqualTo(bType);
  }
}
