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

import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.SchemaElementKind;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.EventExecutor;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the queries to system tables during a schema refresh.
 *
 * <p>Depending on the kind of refresh, there is a variable number of queries. They are all
 * asynchronous, and possibly paged. This class abstracts all the details and exposes a common
 * result type.
 */
public abstract class SchemaQueries {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaQueries.class);
  private static final CassandraVersion CASSANDRA_300 = CassandraVersion.parse("3.0.0");
  private static final TypeCodec<List<String>> LIST_OF_TEXT = TypeCodecs.listOf(TypeCodecs.TEXT);

  public static SchemaQueries newInstance(InternalDriverContext context, String logPrefix) {
    DriverChannel channel = context.controlConnection().channel();
    if (channel == null || channel.closeFuture().isDone()) {
      throw new IllegalStateException("Control channel not available, aborting schema refresh");
    }
    @SuppressWarnings("SuspiciousMethodCalls")
    Node node = context.metadataManager().getMetadata().getNodes().get(channel.remoteAddress());
    if (node == null) {
      throw new IllegalStateException(
          "Could not find control node metadata "
              + channel.remoteAddress()
              + ", aborting schema refresh");
    }
    CassandraVersion cassandraVersion = node.getCassandraVersion().nextStable();
    if (cassandraVersion == null) {
      LOG.warn(
          "[{}] Cassandra version missing for {}, defaulting to {}",
          logPrefix,
          node,
          CASSANDRA_300);
      cassandraVersion = CASSANDRA_300;
    }
    DriverConfigProfile config = context.config().getDefaultProfile();
    LOG.debug(
        "[{}] Sending schema queries to {} with version {}", logPrefix, node, cassandraVersion);
    return (cassandraVersion.compareTo(CASSANDRA_300) < 0)
        ? new Cassandra2SchemaQueries(channel, node, config, logPrefix)
        : new Cassandra3SchemaQueries(channel, node, config, logPrefix);
  }

  private final DriverChannel channel;
  private final EventExecutor adminExecutor;
  private final Node node;
  private final String logPrefix;
  private final Duration timeout;
  private final int pageSize;
  private final CompletableFuture<SchemaRows> schemaRowsFuture = new CompletableFuture<>();

  // All non-final fields are accessed exclusively on adminExecutor
  private SchemaRows.Builder schemaRowsBuilder;
  private int pendingQueries;

  protected SchemaQueries(
      DriverChannel channel, Node node, DriverConfigProfile config, String logPrefix) {
    this.channel = channel;
    this.adminExecutor = channel.eventLoop();
    this.node = node;
    this.logPrefix = logPrefix;
    this.timeout = config.getDuration(CoreDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT);
    this.pageSize = config.getInt(CoreDriverOption.METADATA_SCHEMA_REQUEST_PAGE_SIZE);
  }

  protected abstract String selectKeyspacesQuery();

  protected abstract String selectTablesQuery();

  protected abstract Optional<String> selectViewsQuery();

  protected abstract Optional<String> selectIndexesQuery();

  protected abstract String selectColumnsQuery();

  protected abstract String selectTypesQuery();

  protected abstract String selectFunctionsQuery();

  protected abstract String selectAggregatesQuery();

  protected abstract String tableNameColumn();

  protected abstract String signatureColumn();

  public CompletionStage<SchemaRows> execute(
      SchemaElementKind kind, String keyspace, String object, List<String> arguments) {
    RunOrSchedule.on(
        adminExecutor, () -> executeOnAdminExecutor(kind, keyspace, object, arguments));
    return schemaRowsFuture;
  }

  private void executeOnAdminExecutor(
      SchemaElementKind kind, String keyspace, String object, List<String> arguments) {
    assert adminExecutor.inEventLoop();

    schemaRowsBuilder = new SchemaRows.Builder(node, kind, tableNameColumn(), logPrefix);

    String whereClause = buildWhereClause(kind, keyspace, object, arguments);

    boolean isFullOrKeyspace =
        kind == SchemaElementKind.FULL_SCHEMA || kind == SchemaElementKind.KEYSPACE;
    if (isFullOrKeyspace) {
      query(selectKeyspacesQuery() + whereClause, schemaRowsBuilder::withKeyspaces);
    }
    if (isFullOrKeyspace || kind == SchemaElementKind.TYPE) {
      query(selectTypesQuery() + whereClause, schemaRowsBuilder::withTypes);
    }
    if (isFullOrKeyspace || kind == SchemaElementKind.TABLE) {
      query(selectTablesQuery() + whereClause, schemaRowsBuilder::withTables);
      query(selectColumnsQuery() + whereClause, schemaRowsBuilder::withColumns);
      selectIndexesQuery()
          .ifPresent(select -> query(select + whereClause, schemaRowsBuilder::withIndexes));
      selectViewsQuery()
          .ifPresent(
              select -> {
                // Individual view notifications are sent with the TABLE type, we need to translate
                // to VIEW to generate the appropriate WHERE clause.
                SchemaElementKind whereClauseKind =
                    (kind == SchemaElementKind.TABLE ? SchemaElementKind.VIEW : kind);
                String viewWhereClause =
                    buildWhereClause(whereClauseKind, keyspace, object, arguments);
                query(select + viewWhereClause, schemaRowsBuilder::withViews);
              });
    }
    if (isFullOrKeyspace || kind == SchemaElementKind.FUNCTION) {
      query(selectFunctionsQuery() + whereClause, schemaRowsBuilder::withFunctions);
    }
    if (isFullOrKeyspace || kind == SchemaElementKind.AGGREGATE) {
      query(selectAggregatesQuery() + whereClause, schemaRowsBuilder::withAggregates);
    }
  }

  private void query(
      String queryString, Function<Iterable<AdminResult.Row>, SchemaRows.Builder> builderUpdater) {
    assert adminExecutor.inEventLoop();

    pendingQueries += 1;
    query(queryString)
        .whenCompleteAsync(
            (result, error) -> handleResult(result, error, builderUpdater), adminExecutor);
  }

  @VisibleForTesting
  protected CompletionStage<AdminResult> query(String query) {
    return AdminRequestHandler.query(channel, query, timeout, pageSize, logPrefix).start();
  }

  private void handleResult(
      AdminResult result,
      Throwable error,
      Function<Iterable<AdminResult.Row>, SchemaRows.Builder> builderUpdater) {
    if (schemaRowsFuture.isDone()) { // Another query failed already, ignore
      return;
    }
    if (error != null) {
      // Any error fails the whole refresh
      schemaRowsFuture.completeExceptionally(error);
    } else {
      // Store the rows of the current page in the builder
      schemaRowsBuilder = builderUpdater.apply(result);
      // Move to the next page, or complete if we're the last query
      if (result.hasNextPage()) {
        result
            .nextPage()
            .whenCompleteAsync(
                (nextResult, nextError) -> handleResult(nextResult, nextError, builderUpdater),
                adminExecutor);
      } else {
        pendingQueries -= 1;
        if (pendingQueries == 0) {
          schemaRowsFuture.complete(schemaRowsBuilder.build());
        }
      }
    }
  }

  private String buildWhereClause(
      SchemaElementKind kind, String keyspace, String object, List<String> arguments) {
    if (kind == SchemaElementKind.FULL_SCHEMA) {
      return "";
    } else {
      String whereClause = String.format(" WHERE keyspace_name = '%s'", keyspace);
      if (kind == SchemaElementKind.TABLE) {
        whereClause += String.format(" AND %s = '%s'", tableNameColumn(), object);
      } else if (kind == SchemaElementKind.VIEW) {
        whereClause += String.format(" AND view_name = '%s'", object);
      } else if (kind == SchemaElementKind.TYPE) {
        whereClause += String.format(" AND type_name = '%s'", object);
      } else if (kind == SchemaElementKind.FUNCTION) {
        whereClause +=
            String.format(
                " AND function_name = '%s' AND %s = %s",
                object, signatureColumn(), LIST_OF_TEXT.format(arguments));
      } else if (kind == SchemaElementKind.AGGREGATE) {
        whereClause +=
            String.format(
                " AND aggregate_name = '%s' AND %s = %s",
                object, signatureColumn(), LIST_OF_TEXT.format(arguments));
      }
      return whereClause;
    }
  }
}
