package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.protocol.internal.response.event.SchemaChangeEvent;
import com.datastax.oss.protocol.internal.response.result.SchemaChange;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** A request to refresh schema metadata, that will be processed by {@link MetadataManager}. */
public class SchemaRefreshRequest {

  /** Builds an instance from a server-pushed notification on the control connection. */
  public SchemaRefreshRequest fromEvent(SchemaChangeEvent event) {
    return new SchemaRefreshRequest(
        SchemaChangeType.fromProtocolString(event.changeType),
        SchemaChangeScope.fromProtocolString(event.target),
        event.keyspace,
        event.object,
        event.arguments);
  }

  /** Builds an instance from a {@code SCHEMA_CHANGE} response to a DDL query. */
  public SchemaRefreshRequest fromResponse(SchemaChange response) {
    return new SchemaRefreshRequest(
        SchemaChangeType.fromProtocolString(response.changeType),
        SchemaChangeScope.fromProtocolString(response.target),
        response.keyspace,
        response.object,
        response.arguments);
  }

  public SchemaRefreshRequest full() {
    return new SchemaRefreshRequest(
        SchemaChangeType.UPDATED, SchemaChangeScope.FULL_SCHEMA, null, null, null);
  }

  public final SchemaChangeType type;
  public final SchemaChangeScope scope;
  public final String keyspace;
  public final String object;
  public final List<String> arguments;
  /** A future that will complete with the refreshed metadata. */
  public final CompletableFuture<Metadata> future;

  private SchemaRefreshRequest(
      SchemaChangeType type,
      SchemaChangeScope scope,
      String keyspace,
      String object,
      List<String> arguments) {
    this.type = type;
    this.scope = scope;
    this.keyspace = keyspace;
    this.object = object;
    this.arguments = arguments;
    this.future = new CompletableFuture<>();
  }
}
