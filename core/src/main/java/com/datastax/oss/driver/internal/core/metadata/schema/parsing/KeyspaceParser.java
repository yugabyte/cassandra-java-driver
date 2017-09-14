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
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.SchemaRefresh;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.SingleKeyspaceRefresh;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

class KeyspaceParser extends SchemaElementParser {

  private final SchemaParser parent;
  private final TableParser tableParser;
  private final ViewParser viewParser;
  private final FunctionParser functionParser;
  private final AggregateParser aggregateParser;

  KeyspaceParser(SchemaParser parent) {
    super(parent);
    this.parent = parent;
    this.tableParser = new TableParser(parent);
    this.viewParser = new ViewParser(parent);
    this.functionParser = new FunctionParser(parent);
    this.aggregateParser = new AggregateParser(parent);
  }

  SchemaRefresh parse() {
    if (rows.keyspaces.size() != 1) {
      throw new IllegalArgumentException(
          "Processing a KEYSPACE refresh, expecting exactly one keyspace row but found "
              + rows.keyspaces.size());
    }

    AdminRow keyspaceRow = rows.keyspaces.get(0);

    KeyspaceMetadata keyspace = parseKeyspace(keyspaceRow);
    return (keyspace == null)
        ? null
        : new SingleKeyspaceRefresh(currentMetadata, rows.request, keyspace, logPrefix);
  }

  // Note that the server only issues KEYSPACE UPDATED events when an ALTER KEYSPACE command has
  // been run.
  // However the driver also debounces multiple element updates in the same keyspace into a full
  // keyspace refresh, so we interpret an update as "the keyspace _and_ all its elements".
  //
  // In practice ALTER KEYSPACE commands are relatively rare, so in most cases when we get a
  // KEYSPACE UPDATED it will come from the debouncer.
  KeyspaceMetadata parseKeyspace(AdminRow keyspaceRow) {

    // Cassandra <= 2.2
    // CREATE TABLE system.schema_keyspaces (
    //     keyspace_name text PRIMARY KEY,
    //     durable_writes boolean,
    //     strategy_class text,
    //     strategy_options text
    // )
    //
    // Cassandra >= 3.0:
    // CREATE TABLE system_schema.keyspaces (
    //     keyspace_name text PRIMARY KEY,
    //     durable_writes boolean,
    //     replication frozen<map<text, text>>
    // )
    CqlIdentifier keyspaceId = CqlIdentifier.fromInternal(keyspaceRow.getString("keyspace_name"));
    boolean durableWrites =
        MoreObjects.firstNonNull(keyspaceRow.getBoolean("durable_writes"), false);

    Map<String, String> replicationOptions;
    if (keyspaceRow.contains("strategy_class")) {
      String strategyClass = keyspaceRow.getString("strategy_class");
      Map<String, String> strategyOptions =
          SimpleJsonParser.parseStringMap(keyspaceRow.getString("strategy_options"));
      replicationOptions =
          ImmutableMap.<String, String>builder()
              .putAll(strategyOptions)
              .put("class", strategyClass)
              .build();
    } else {
      replicationOptions = keyspaceRow.getMapOfStringToString("replication");
    }

    Map<CqlIdentifier, UserDefinedType> types =
        new TypeListParser(parent, keyspaceId).parse(rows.types.get(keyspaceId));

    ImmutableMap.Builder<CqlIdentifier, TableMetadata> tablesBuilder = ImmutableMap.builder();
    for (AdminRow tableRow : rows.tables.get(keyspaceId)) {
      TableMetadata table = tableParser.parseTable(tableRow, keyspaceId, types);
      tablesBuilder.put(table.getName(), table);
    }

    ImmutableMap.Builder<CqlIdentifier, ViewMetadata> viewsBuilder = ImmutableMap.builder();
    for (AdminRow viewRow : rows.views.get(keyspaceId)) {
      ViewMetadata view = viewParser.parseView(viewRow, keyspaceId, types);
      viewsBuilder.put(view.getName(), view);
    }

    ImmutableMap.Builder<FunctionSignature, FunctionMetadata> functionsBuilder =
        ImmutableMap.builder();
    for (AdminRow functionRow : rows.functions.get(keyspaceId)) {
      FunctionMetadata function = functionParser.parseRow(functionRow, keyspaceId, types);
      functionsBuilder.put(function.getSignature(), function);
    }

    ImmutableMap.Builder<FunctionSignature, AggregateMetadata> aggregatesBuilder =
        ImmutableMap.builder();
    for (AdminRow aggregateRow : rows.aggregates.get(keyspaceId)) {
      AggregateMetadata aggregate = aggregateParser.parseRow(aggregateRow, keyspaceId, types);
      aggregatesBuilder.put(aggregate.getSignature(), aggregate);
    }

    return new DefaultKeyspaceMetadata(
        keyspaceId,
        durableWrites,
        replicationOptions,
        types,
        tablesBuilder.build(),
        viewsBuilder.build(),
        functionsBuilder.build(),
        aggregatesBuilder.build());
  }
}
