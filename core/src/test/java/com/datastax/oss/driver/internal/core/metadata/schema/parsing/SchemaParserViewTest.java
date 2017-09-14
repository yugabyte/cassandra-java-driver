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
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeScope;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaRefreshRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.ViewRefresh;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Iterator;
import org.junit.Test;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;

public class SchemaParserViewTest extends SchemaParserTestBase {

  static final AdminRow VIEW_ROW_3_0 =
      mockViewRow("ks", "alltimehigh", "scores", false, "game IS NOT NULL");
  static final Iterable<AdminRow> COLUMN_ROWS_3_0 =
      ImmutableList.of(
          mockModernColumnRow("ks", "alltimehigh", "game", "partition_key", "text", "none", 0),
          mockModernColumnRow("ks", "alltimehigh", "score", "clustering", "int", "desc", 0),
          mockModernColumnRow("ks", "alltimehigh", "user", "clustering", "text", "asc", 1),
          mockModernColumnRow("ks", "alltimehigh", "year", "clustering", "int", "asc", 2),
          mockModernColumnRow("ks", "alltimehigh", "month", "clustering", "int", "asc", 3),
          mockModernColumnRow("ks", "alltimehigh", "day", "clustering", "int", "asc", 4));

  private static final SchemaRefreshRequest REQUEST =
      new SchemaRefreshRequest(
          SchemaChangeType.UPDATED, SchemaChangeScope.VIEW, "ks", "alltimehigh", null);

  @Test
  public void should_skip_when_keyspace_unknown() {
    assertThat(parse(VIEW_ROW_3_0, Collections.emptyList(), REQUEST)).isNull();
  }

  @Test
  public void should_skip_when_no_column_rows() {
    KeyspaceMetadata ks = Mockito.mock(KeyspaceMetadata.class);
    Mockito.when(currentMetadata.getKeyspaces())
        .thenReturn(ImmutableMap.of(CqlIdentifier.fromInternal("ks"), ks));

    assertThat(parse(VIEW_ROW_3_0, Collections.emptyList(), REQUEST)).isNull();
  }

  @Test
  public void should_parse_view() {
    KeyspaceMetadata ks = Mockito.mock(KeyspaceMetadata.class);
    Mockito.when(currentMetadata.getKeyspaces())
        .thenReturn(ImmutableMap.of(CqlIdentifier.fromInternal("ks"), ks));

    ViewRefresh refresh = (ViewRefresh) parse(VIEW_ROW_3_0, COLUMN_ROWS_3_0, REQUEST);
    assertThat(refresh.request).isEqualTo(REQUEST);

    ViewMetadata view = refresh.newElement;

    assertThat(view.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(view.getName().asInternal()).isEqualTo("alltimehigh");
    assertThat(view.getBaseTable().asInternal()).isEqualTo("scores");

    assertThat(view.getPartitionKey()).hasSize(1);
    ColumnMetadata pk0 = view.getPartitionKey().get(0);
    assertThat(pk0.getName().asInternal()).isEqualTo("game");
    assertThat(pk0.getType()).isEqualTo(DataTypes.TEXT);

    assertThat(view.getClusteringColumns().entrySet()).hasSize(5);
    Iterator<ColumnMetadata> clusteringColumnsIterator =
        view.getClusteringColumns().keySet().iterator();
    assertThat(clusteringColumnsIterator.next().getName().asInternal()).isEqualTo("score");
    assertThat(clusteringColumnsIterator.next().getName().asInternal()).isEqualTo("user");
    assertThat(clusteringColumnsIterator.next().getName().asInternal()).isEqualTo("year");
    assertThat(clusteringColumnsIterator.next().getName().asInternal()).isEqualTo("month");
    assertThat(clusteringColumnsIterator.next().getName().asInternal()).isEqualTo("day");

    assertThat(view.getColumns())
        .containsOnlyKeys(
            CqlIdentifier.fromInternal("game"),
            CqlIdentifier.fromInternal("score"),
            CqlIdentifier.fromInternal("user"),
            CqlIdentifier.fromInternal("year"),
            CqlIdentifier.fromInternal("month"),
            CqlIdentifier.fromInternal("day"));
  }

  private MetadataRefresh parse(
      AdminRow viewRow, Iterable<AdminRow> columnRows, SchemaRefreshRequest request) {
    SchemaRows rows =
        new SchemaRows.Builder(node, request, "table_name", "test")
            .withViews(ImmutableList.of(viewRow))
            .withColumns(columnRows)
            .build();
    return new SchemaParser(rows, currentMetadata, context, "test").parse();
  }
}
