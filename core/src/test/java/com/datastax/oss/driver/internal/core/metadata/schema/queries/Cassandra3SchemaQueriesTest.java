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
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeScope;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingDeque;
import org.junit.Before;
import org.junit.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class Cassandra3SchemaQueriesTest extends SchemaQueriesTest {

  private SchemaQueriesWithMockedChannel queries;

  @Before
  public void setup() {
    super.setup();
    queries = new SchemaQueriesWithMockedChannel(driverChannel, node, config, "test");
  }

  @Test
  public void should_query_type() {
    CompletionStage<SchemaRows> result =
        queries.execute(SchemaChangeType.UPDATED, SchemaChangeScope.TYPE, "ks", "type", null);

    Call call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.types WHERE keyspace_name = 'ks' AND type_name = 'type'");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks", "type_name", "type")));

    channel.runPendingTasks();

    assertThat(result)
        .isSuccess(
            rows -> {
              assertThat(rows.types.keySet()).containsOnly(KS_ID);
              assertThat(rows.types.get(KS_ID)).hasSize(1);
              assertThat(rows.types.get(KS_ID).iterator().next().getString("type_name"))
                  .isEqualTo("type");
            });
  }

  @Test
  public void should_query_function() {
    CompletionStage<SchemaRows> result =
        queries.execute(
            SchemaChangeType.UPDATED,
            SchemaChangeScope.FUNCTION,
            "ks",
            "add",
            ImmutableList.of("int", "int"));

    Call call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.functions "
                + "WHERE keyspace_name = 'ks' AND function_name = 'add' "
                + "AND argument_types = ['int','int']");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks", "function_name", "add")));

    channel.runPendingTasks();

    assertThat(result)
        .isSuccess(
            rows -> {
              assertThat(rows.functions.keySet()).containsOnly(KS_ID);
              assertThat(rows.functions.get(KS_ID)).hasSize(1);
              assertThat(rows.functions.get(KS_ID).iterator().next().getString("function_name"))
                  .isEqualTo("add");
            });
  }

  @Test
  public void should_query_aggregate() {
    CompletionStage<SchemaRows> result =
        queries.execute(
            SchemaChangeType.UPDATED,
            SchemaChangeScope.AGGREGATE,
            "ks",
            "add",
            ImmutableList.of("int", "int"));

    Call call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.aggregates "
                + "WHERE keyspace_name = 'ks' AND aggregate_name = 'add' "
                + "AND argument_types = ['int','int']");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks", "aggregate_name", "add")));

    channel.runPendingTasks();

    assertThat(result)
        .isSuccess(
            rows -> {
              assertThat(rows.aggregates.keySet()).containsOnly(KS_ID);
              assertThat(rows.aggregates.get(KS_ID)).hasSize(1);
              assertThat(rows.aggregates.get(KS_ID).iterator().next().getString("aggregate_name"))
                  .isEqualTo("add");
            });
  }

  @Test
  public void should_query_table() {
    CompletionStage<SchemaRows> result =
        queries.execute(SchemaChangeType.UPDATED, SchemaChangeScope.TABLE, "ks", "foo", null);

    // Table
    Call call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.tables WHERE keyspace_name = 'ks' AND table_name = 'foo'");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks", "table_name", "foo")));

    // Columns
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.columns "
                + "WHERE keyspace_name = 'ks' AND table_name = 'foo'");
    call.result.complete(
        mockResult(mockRow("keyspace_name", "ks", "table_name", "foo", "column_name", "k")));

    // Indexes
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.indexes "
                + "WHERE keyspace_name = 'ks' AND table_name = 'foo'");
    call.result.complete(
        mockResult(mockRow("keyspace_name", "ks", "table_name", "foo", "index_name", "index")));

    // Views
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.views WHERE keyspace_name = 'ks' AND view_name = 'foo'");
    call.result.complete(mockResult(/*no rows*/ ));

    channel.runPendingTasks();

    assertThat(result)
        .isSuccess(
            rows -> {
              assertThat(rows.scope).isEqualTo(SchemaChangeScope.TABLE);

              assertThat(rows.tables.keySet()).containsOnly(KS_ID);
              assertThat(rows.tables.get(KS_ID)).hasSize(1);
              assertThat(rows.tables.get(KS_ID).iterator().next().getString("table_name"))
                  .isEqualTo("foo");

              assertThat(rows.columns.keySet()).containsOnly(KS_ID);
              assertThat(rows.columns.get(KS_ID).keySet()).containsOnly(FOO_ID);
              assertThat(
                      rows.columns
                          .get(KS_ID)
                          .get(FOO_ID)
                          .iterator()
                          .next()
                          .getString("column_name"))
                  .isEqualTo("k");

              assertThat(rows.indexes.keySet()).containsOnly(KS_ID);
              assertThat(rows.indexes.get(KS_ID).keySet()).containsOnly(FOO_ID);
              assertThat(
                      rows.indexes.get(KS_ID).get(FOO_ID).iterator().next().getString("index_name"))
                  .isEqualTo("index");

              assertThat(rows.views.keySet()).isEmpty();
            });
  }

  @Test
  public void should_query_view() {
    CompletionStage<SchemaRows> result =
        queries.execute(SchemaChangeType.UPDATED, SchemaChangeScope.TABLE, "ks", "foo", null);

    // Table
    Call call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.tables WHERE keyspace_name = 'ks' AND table_name = 'foo'");
    call.result.complete(mockResult(/* no rows */ ));

    // Columns
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.columns "
                + "WHERE keyspace_name = 'ks' AND table_name = 'foo'");
    call.result.complete(
        mockResult(mockRow("keyspace_name", "ks", "table_name", "foo", "column_name", "k")));

    // Indexes
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.indexes "
                + "WHERE keyspace_name = 'ks' AND table_name = 'foo'");
    call.result.complete(
        mockResult(mockRow("keyspace_name", "ks", "table_name", "foo", "index_name", "index")));

    // Views
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.views WHERE keyspace_name = 'ks' AND view_name = 'foo'");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks", "view_name", "foo")));

    channel.runPendingTasks();

    assertThat(result)
        .isSuccess(
            rows -> {
              assertThat(rows.scope).isEqualTo(SchemaChangeScope.VIEW);

              assertThat(rows.tables.keySet()).isEmpty();

              assertThat(rows.views.keySet()).containsOnly(KS_ID);
              assertThat(rows.views.get(KS_ID)).hasSize(1);
              assertThat(rows.views.get(KS_ID).iterator().next().getString("view_name"))
                  .isEqualTo("foo");

              assertThat(rows.columns.keySet()).containsOnly(KS_ID);
              assertThat(rows.columns.get(KS_ID).keySet()).containsOnly(FOO_ID);
              assertThat(
                      rows.columns
                          .get(KS_ID)
                          .get(FOO_ID)
                          .iterator()
                          .next()
                          .getString("column_name"))
                  .isEqualTo("k");

              assertThat(rows.indexes.keySet()).containsOnly(KS_ID);
              assertThat(rows.indexes.get(KS_ID).keySet()).containsOnly(FOO_ID);
              assertThat(
                      rows.indexes.get(KS_ID).get(FOO_ID).iterator().next().getString("index_name"))
                  .isEqualTo("index");
            });
  }

  @Test
  public void should_query_keyspace() {
    CompletionStage<SchemaRows> result =
        queries.execute(SchemaChangeType.UPDATED, SchemaChangeScope.KEYSPACE, "ks", null, null);

    // Keyspace
    Call call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'ks'");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks")));

    // Types
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.types WHERE keyspace_name = 'ks'");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks", "type_name", "type")));

    // Tables
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.tables WHERE keyspace_name = 'ks'");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks", "table_name", "foo")));

    // Columns
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.columns WHERE keyspace_name = 'ks'");
    call.result.complete(
        mockResult(mockRow("keyspace_name", "ks", "table_name", "foo", "column_name", "k")));

    // Indexes
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.indexes WHERE keyspace_name = 'ks'");
    call.result.complete(
        mockResult(mockRow("keyspace_name", "ks", "table_name", "foo", "index_name", "index")));

    // Views
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.views WHERE keyspace_name = 'ks'");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks", "view_name", "foo")));

    // Functions
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.functions WHERE keyspace_name = 'ks'");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks", "function_name", "add")));

    // Aggregates
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.aggregates WHERE keyspace_name = 'ks'");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks", "aggregate_name", "add")));

    channel.runPendingTasks();

    assertThat(result)
        .isSuccess(
            rows -> {
              // Keyspace
              assertThat(rows.keyspaces).hasSize(1);
              assertThat(rows.keyspaces.get(0).getString("keyspace_name")).isEqualTo("ks");

              // Types
              assertThat(rows.types.keySet()).containsOnly(KS_ID);
              assertThat(rows.types.get(KS_ID)).hasSize(1);
              assertThat(rows.types.get(KS_ID).iterator().next().getString("type_name"))
                  .isEqualTo("type");

              // Tables
              assertThat(rows.tables.keySet()).containsOnly(KS_ID);
              assertThat(rows.tables.get(KS_ID)).hasSize(1);
              assertThat(rows.tables.get(KS_ID).iterator().next().getString("table_name"))
                  .isEqualTo("foo");

              // Rows
              assertThat(rows.columns.keySet()).containsOnly(KS_ID);
              assertThat(rows.columns.get(KS_ID).keySet()).containsOnly(FOO_ID);
              assertThat(
                      rows.columns
                          .get(KS_ID)
                          .get(FOO_ID)
                          .iterator()
                          .next()
                          .getString("column_name"))
                  .isEqualTo("k");

              // Indexes
              assertThat(rows.indexes.keySet()).containsOnly(KS_ID);
              assertThat(rows.indexes.get(KS_ID).keySet()).containsOnly(FOO_ID);
              assertThat(
                      rows.indexes.get(KS_ID).get(FOO_ID).iterator().next().getString("index_name"))
                  .isEqualTo("index");

              // Views
              assertThat(rows.views.keySet()).containsOnly(KS_ID);
              assertThat(rows.views.get(KS_ID)).hasSize(1);
              assertThat(rows.views.get(KS_ID).iterator().next().getString("view_name"))
                  .isEqualTo("foo");

              // Functions
              assertThat(rows.functions.keySet()).containsOnly(KS_ID);
              assertThat(rows.functions.get(KS_ID)).hasSize(1);
              assertThat(rows.functions.get(KS_ID).iterator().next().getString("function_name"))
                  .isEqualTo("add");

              // Aggregates
              assertThat(rows.aggregates.keySet()).containsOnly(KS_ID);
              assertThat(rows.aggregates.get(KS_ID)).hasSize(1);
              assertThat(rows.aggregates.get(KS_ID).iterator().next().getString("aggregate_name"))
                  .isEqualTo("add");
            });
  }

  @Test
  public void should_query_full_schema() {
    CompletionStage<SchemaRows> result =
        queries.execute(SchemaChangeType.UPDATED, SchemaChangeScope.FULL_SCHEMA, null, null, null);

    // Keyspace
    Call call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.keyspaces");
    call.result.complete(
        mockResult(mockRow("keyspace_name", "ks1"), mockRow("keyspace_name", "ks2")));

    // Types
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.types");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks1", "type_name", "type")));

    // Tables
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.tables");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks1", "table_name", "foo")));

    // Columns
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.columns");
    call.result.complete(
        mockResult(mockRow("keyspace_name", "ks1", "table_name", "foo", "column_name", "k")));

    // Indexes
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.indexes");
    call.result.complete(
        mockResult(mockRow("keyspace_name", "ks1", "table_name", "foo", "index_name", "index")));

    // Views
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.views");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks2", "view_name", "foo")));

    // Functions
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.functions");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks2", "function_name", "add")));

    // Aggregates
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.aggregates");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks2", "aggregate_name", "add")));

    channel.runPendingTasks();

    assertThat(result)
        .isSuccess(
            rows -> {
              // Keyspace
              assertThat(rows.keyspaces).hasSize(2);
              assertThat(rows.keyspaces.get(0).getString("keyspace_name")).isEqualTo("ks1");
              assertThat(rows.keyspaces.get(1).getString("keyspace_name")).isEqualTo("ks2");

              // Types
              assertThat(rows.types.keySet()).containsOnly(KS1_ID);
              assertThat(rows.types.get(KS1_ID)).hasSize(1);
              assertThat(rows.types.get(KS1_ID).iterator().next().getString("type_name"))
                  .isEqualTo("type");

              // Tables
              assertThat(rows.tables.keySet()).containsOnly(KS1_ID);
              assertThat(rows.tables.get(KS1_ID)).hasSize(1);
              assertThat(rows.tables.get(KS1_ID).iterator().next().getString("table_name"))
                  .isEqualTo("foo");

              // Rows
              assertThat(rows.columns.keySet()).containsOnly(KS1_ID);
              assertThat(rows.columns.get(KS1_ID).keySet()).containsOnly(FOO_ID);
              assertThat(
                      rows.columns
                          .get(KS1_ID)
                          .get(FOO_ID)
                          .iterator()
                          .next()
                          .getString("column_name"))
                  .isEqualTo("k");

              // Indexes
              assertThat(rows.indexes.keySet()).containsOnly(KS1_ID);
              assertThat(rows.indexes.get(KS1_ID).keySet()).containsOnly(FOO_ID);
              assertThat(
                      rows.indexes
                          .get(KS1_ID)
                          .get(FOO_ID)
                          .iterator()
                          .next()
                          .getString("index_name"))
                  .isEqualTo("index");

              // Views
              assertThat(rows.views.keySet()).containsOnly(KS2_ID);
              assertThat(rows.views.get(KS2_ID)).hasSize(1);
              assertThat(rows.views.get(KS2_ID).iterator().next().getString("view_name"))
                  .isEqualTo("foo");

              // Functions
              assertThat(rows.functions.keySet()).containsOnly(KS2_ID);
              assertThat(rows.functions.get(KS2_ID)).hasSize(1);
              assertThat(rows.functions.get(KS2_ID).iterator().next().getString("function_name"))
                  .isEqualTo("add");

              // Aggregates
              assertThat(rows.aggregates.keySet()).containsOnly(KS2_ID);
              assertThat(rows.aggregates.get(KS2_ID)).hasSize(1);
              assertThat(rows.aggregates.get(KS2_ID).iterator().next().getString("aggregate_name"))
                  .isEqualTo("add");
            });
  }

  @Test
  public void should_query_with_paging() {
    // We're cheating a bit to simplify the test: in real life a type query would always return at
    // most one row, queries can only be paged for full schema, keyspace or table (via its columns).
    // But those scenarios require more queries to mock, so use type instead (the underlying logic
    // is shared).
    CompletionStage<SchemaRows> result =
        queries.execute(SchemaChangeType.UPDATED, SchemaChangeScope.TYPE, "ks", "type", null);

    Call call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.types "
                + "WHERE keyspace_name = 'ks' AND type_name = 'type'");
    AdminResult page2 = mockResult(mockRow("keyspace_name", "ks", "type_name", "type2"));
    AdminResult page1 = mockResult(page2, mockRow("keyspace_name", "ks", "type_name", "type1"));
    call.result.complete(page1);

    channel.runPendingTasks();

    assertThat(result)
        .isSuccess(
            rows -> {
              assertThat(rows.types.keySet()).containsOnly(KS_ID);
              assertThat(rows.types.get(KS_ID)).hasSize(2);
              Iterator<AdminRow> iterator = rows.types.get(KS_ID).iterator();
              assertThat(iterator.next().getString("type_name")).isEqualTo("type1");
              assertThat(iterator.next().getString("type_name")).isEqualTo("type2");
            });
  }

  @Test
  public void should_ignore_malformed_rows() {
    CompletionStage<SchemaRows> result =
        queries.execute(SchemaChangeType.UPDATED, SchemaChangeScope.TABLE, "ks", "foo", null);

    // Table
    Call call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.tables "
                + "WHERE keyspace_name = 'ks' AND table_name = 'foo'");
    call.result.complete(
        mockResult(
            mockRow("keyspace_name", "ks", "table_name", "foo"),
            // Missing keyspace name:
            mockRow("table_name", "foo")));

    // Columns
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.columns "
                + "WHERE keyspace_name = 'ks' AND table_name = 'foo'");
    call.result.complete(
        mockResult(
            mockRow("keyspace_name", "ks", "table_name", "foo", "column_name", "k"),
            // Missing keyspace name:
            mockRow("table_name", "foo", "column_name", "k"),
            // Missing table name:
            mockRow("keyspace_name", "ks", "column_name", "k")));

    // Indexes
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.indexes "
                + "WHERE keyspace_name = 'ks' AND table_name = 'foo'");
    call.result.complete(mockResult());

    // Views
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo(
            "SELECT * FROM system_schema.views "
                + "WHERE keyspace_name = 'ks' AND view_name = 'foo'");
    call.result.complete(mockResult());

    channel.runPendingTasks();

    assertThat(result)
        .isSuccess(
            rows -> {
              assertThat(rows.tables.keySet()).containsOnly(KS_ID);
              assertThat(rows.tables.get(KS_ID)).hasSize(1);
              assertThat(rows.tables.get(KS_ID).iterator().next().getString("table_name"))
                  .isEqualTo("foo");

              assertThat(rows.columns.keySet()).containsOnly(KS_ID);
              assertThat(rows.columns.get(KS_ID).keySet()).containsOnly(FOO_ID);
              assertThat(
                      rows.columns
                          .get(KS_ID)
                          .get(FOO_ID)
                          .iterator()
                          .next()
                          .getString("column_name"))
                  .isEqualTo("k");
            });
  }

  /** Extends the class under test to mock the query execution logic. */
  static class SchemaQueriesWithMockedChannel extends Cassandra3SchemaQueries {

    final Queue<Call> calls = new LinkedBlockingDeque<>();

    SchemaQueriesWithMockedChannel(
        DriverChannel channel, Node node, DriverConfigProfile config, String logPrefix) {
      super(channel, node, config, logPrefix);
    }

    @Override
    protected CompletionStage<AdminResult> query(String query) {
      Call call = new Call(query);
      calls.add(call);
      return call.result;
    }
  }
}
