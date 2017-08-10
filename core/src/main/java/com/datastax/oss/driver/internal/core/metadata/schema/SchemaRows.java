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
package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult.Row;
import com.datastax.oss.driver.internal.core.metadata.SchemaElementKind;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gathers all the rows returned by the queries for a schema refresh, categorizing them by
 * keyspace/table where relevant.
 */
public class SchemaRows {

  /** The node we got the data from */
  public final Node node;

  public final SchemaElementKind refreshKind;
  public final List<Row> keyspaces;
  public final Multimap<String, Row> tables;
  public final Multimap<String, Row> views;
  public final Multimap<String, Row> types;
  public final Multimap<String, Row> functions;
  public final Multimap<String, Row> aggregates;
  public final Map<String, Multimap<String, Row>> columns;
  public final Map<String, Multimap<String, Row>> indexes;

  private SchemaRows(
      Node node,
      SchemaElementKind refreshKind,
      List<Row> keyspaces,
      Multimap<String, Row> tables,
      Multimap<String, Row> views,
      Map<String, Multimap<String, Row>> columns,
      Map<String, Multimap<String, Row>> indexes,
      Multimap<String, Row> types,
      Multimap<String, Row> functions,
      Multimap<String, Row> aggregates) {
    this.node = node;
    this.refreshKind = refreshKind;
    this.keyspaces = keyspaces;
    this.tables = tables;
    this.views = views;
    this.columns = columns;
    this.indexes = indexes;
    this.types = types;
    this.functions = functions;
    this.aggregates = aggregates;
  }

  static class Builder {
    private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

    private final Node node;
    private final SchemaElementKind refreshKind;
    private final String tableNameColumn;
    private final String logPrefix;
    private final ImmutableList.Builder<Row> keyspacesBuilder = ImmutableList.builder();
    private final ImmutableMultimap.Builder<String, Row> tablesBuilder =
        ImmutableListMultimap.builder();
    private final ImmutableMultimap.Builder<String, Row> viewsBuilder =
        ImmutableListMultimap.builder();
    private final ImmutableMultimap.Builder<String, Row> typesBuilder =
        ImmutableListMultimap.builder();
    private final ImmutableMultimap.Builder<String, Row> functionsBuilder =
        ImmutableListMultimap.builder();
    private final ImmutableMultimap.Builder<String, Row> aggregatesBuilder =
        ImmutableListMultimap.builder();
    private final Map<String, ImmutableMultimap.Builder<String, Row>> columnsBuilders =
        new LinkedHashMap<>();
    private final Map<String, ImmutableMultimap.Builder<String, Row>> indexesBuilders =
        new LinkedHashMap<>();

    Builder(Node node, SchemaElementKind refreshKind, String tableNameColumn, String logPrefix) {
      this.node = node;
      this.refreshKind = refreshKind;
      this.tableNameColumn = tableNameColumn;
      this.logPrefix = logPrefix;
    }

    Builder withKeyspaces(Iterable<Row> rows) {
      keyspacesBuilder.addAll(rows);
      return this;
    }

    Builder withTables(Iterable<Row> rows) {
      for (Row row : rows) {
        putByKeyspace(row, tablesBuilder);
      }
      return this;
    }

    Builder withViews(Iterable<Row> rows) {
      for (Row row : rows) {
        putByKeyspace(row, viewsBuilder);
      }
      return this;
    }

    Builder withTypes(Iterable<Row> rows) {
      for (Row row : rows) {
        putByKeyspace(row, typesBuilder);
      }
      return this;
    }

    Builder withFunctions(Iterable<Row> rows) {
      for (Row row : rows) {
        putByKeyspace(row, functionsBuilder);
      }
      return this;
    }

    Builder withAggregates(Iterable<Row> rows) {
      for (Row row : rows) {
        putByKeyspace(row, aggregatesBuilder);
      }
      return this;
    }

    Builder withColumns(Iterable<Row> rows) {
      for (Row row : rows) {
        putByKeyspaceAndTable(row, columnsBuilders);
      }
      return this;
    }

    Builder withIndexes(Iterable<Row> rows) {
      for (Row row : rows) {
        putByKeyspaceAndTable(row, indexesBuilders);
      }
      return this;
    }

    private void putByKeyspace(Row row, ImmutableMultimap.Builder<String, Row> builder) {
      String keyspace = row.getString("keyspace_name");
      if (keyspace == null) {
        LOG.warn("[{}] Skipping system row with missing keyspace name", logPrefix);
      } else {
        builder.put(keyspace, row);
      }
    }

    private void putByKeyspaceAndTable(
        Row row, Map<String, ImmutableMultimap.Builder<String, Row>> builders) {
      String keyspace = row.getString("keyspace_name");
      String table = row.getString(tableNameColumn);
      if (keyspace == null) {
        LOG.warn("[{}] Skipping system row with missing keyspace name", logPrefix);
      } else if (table == null) {
        LOG.warn("[{}] Skipping system row with missing table name", logPrefix);
      } else {
        ImmutableMultimap.Builder<String, Row> builder =
            builders.computeIfAbsent(keyspace, s -> ImmutableListMultimap.builder());
        builder.put(table, row);
      }
    }

    SchemaRows build() {
      return new SchemaRows(
          node,
          refreshKind,
          keyspacesBuilder.build(),
          tablesBuilder.build(),
          viewsBuilder.build(),
          build(columnsBuilders),
          build(indexesBuilders),
          typesBuilder.build(),
          functionsBuilder.build(),
          aggregatesBuilder.build());
    }

    private static <K1, K2, V> Map<K1, Multimap<K2, V>> build(
        Map<K1, ImmutableMultimap.Builder<K2, V>> builders) {
      ImmutableMap.Builder<K1, Multimap<K2, V>> builder = ImmutableMap.builder();
      for (Map.Entry<K1, ImmutableMultimap.Builder<K2, V>> entry : builders.entrySet()) {
        builder.put(entry.getKey(), entry.getValue().build());
      }
      return builder.build();
    }
  }
}
