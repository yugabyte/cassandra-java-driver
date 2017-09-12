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
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeScope;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.SingleKeyspaceRefresh;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.google.common.collect.ImmutableList;
import java.util.function.Consumer;
import org.junit.Test;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;

public class SchemaParserKeyspaceTest extends SchemaParserTest {

  @Test(expected = IllegalArgumentException.class)
  public void should_skip_when_no_rows() {
    assertThat(parse(rows -> {})).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_skip_when_too_many_rows() {
    assertThat(
            parse(
                rows ->
                    rows.withKeyspaces(
                        ImmutableList.of(
                            mockModernKeyspaceRow("ks1"), mockModernKeyspaceRow("ks2")))))
        .isNull();
  }

  @Test
  public void should_parse_modern_keyspace_row() {
    SingleKeyspaceRefresh refresh =
        (SingleKeyspaceRefresh)
            parse(rows -> rows.withKeyspaces(ImmutableList.of(mockModernKeyspaceRow("ks"))));

    assertThat(refresh.changeType).isEqualTo(SchemaChangeType.UPDATED);

    KeyspaceMetadata keyspace = refresh.newKeyspace;
    checkKeyspace(keyspace);
  }

  @Test
  public void should_parse_legacy_keyspace_row() {
    SingleKeyspaceRefresh refresh =
        (SingleKeyspaceRefresh)
            parse(rows -> rows.withKeyspaces(ImmutableList.of(mockLegacyKeyspaceRow("ks"))));

    assertThat(refresh.changeType).isEqualTo(SchemaChangeType.UPDATED);

    KeyspaceMetadata keyspace = refresh.newKeyspace;
    checkKeyspace(keyspace);
  }

  @Test
  public void should_parse_keyspace_with_all_children() {
    // Needed to parse the aggregate
    Mockito.when(context.codecRegistry()).thenReturn(new DefaultCodecRegistry("test"));

    SingleKeyspaceRefresh refresh =
        (SingleKeyspaceRefresh)
            parse(
                rows ->
                    rows.withKeyspaces(ImmutableList.of(mockModernKeyspaceRow("ks")))
                        .withTypes(
                            ImmutableList.of(
                                mockTypeRow(
                                    "ks", "t", ImmutableList.of("i"), ImmutableList.of("int"))))
                        .withTables(ImmutableList.of(SchemaParserTableTest.TABLE_ROW_3_0))
                        .withColumns(SchemaParserTableTest.COLUMN_ROWS_3_0)
                        .withIndexes(SchemaParserTableTest.INDEX_ROWS_3_0)
                        .withViews(ImmutableList.of(SchemaParserViewTest.VIEW_ROW_3_0))
                        .withColumns(SchemaParserViewTest.COLUMN_ROWS_3_0)
                        .withFunctions(ImmutableList.of(SchemaParserFunctionTest.ID_ROW_3_0))
                        .withAggregates(
                            ImmutableList.of(SchemaParserAggregateTest.SUM_AND_TO_STRING_ROW_3_0)));

    assertThat(refresh.changeType).isEqualTo(SchemaChangeType.UPDATED);

    KeyspaceMetadata keyspace = refresh.newKeyspace;
    checkKeyspace(keyspace);

    assertThat(keyspace.getUserDefinedTypes())
        .hasSize(1)
        .containsKey(CqlIdentifier.fromInternal("t"));
    assertThat(keyspace.getTables()).hasSize(1).containsKey(CqlIdentifier.fromInternal("foo"));
    assertThat(keyspace.getViews())
        .hasSize(1)
        .containsKey(CqlIdentifier.fromInternal("alltimehigh"));
    assertThat(keyspace.getFunctions())
        .hasSize(1)
        .containsKey(new FunctionSignature(CqlIdentifier.fromInternal("id"), DataTypes.INT));
    assertThat(keyspace.getAggregates())
        .hasSize(1)
        .containsKey(
            new FunctionSignature(CqlIdentifier.fromInternal("sum_and_to_string"), DataTypes.INT));
  }

  // Common assertions, the keyspace has the same info in all of our examples
  private void checkKeyspace(KeyspaceMetadata keyspace) {
    assertThat(keyspace.getName().asInternal()).isEqualTo("ks");
    assertThat(keyspace.isDurableWrites()).isTrue();
    assertThat(keyspace.getReplication())
        .hasSize(2)
        .containsEntry("class", "org.apache.cassandra.locator.SimpleStrategy")
        .containsEntry("replication_factor", "1");
  }

  private MetadataRefresh parse(Consumer<SchemaRows.Builder> builderConfig) {
    SchemaRows.Builder builder =
        new SchemaRows.Builder(
            node, SchemaChangeType.UPDATED, SchemaChangeScope.KEYSPACE, "table_name", "test");
    builderConfig.accept(builder);
    SchemaRows rows = builder.build();
    return new SchemaParser(rows, currentMetadata, context, "test").parse();
  }
}
