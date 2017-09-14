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

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.protocol.internal.response.event.SchemaChangeEvent;
import com.datastax.oss.protocol.internal.response.result.SchemaChange;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** A request to refresh schema metadata, that will be processed by {@link MetadataManager}. */
public class SchemaRefreshRequest {

  /** Builds an instance from a server-pushed notification on the control connection. */
  public static SchemaRefreshRequest fromEvent(SchemaChangeEvent event) {
    return new SchemaRefreshRequest(
        SchemaChangeType.fromProtocolString(event.changeType),
        SchemaChangeScope.fromProtocolString(event.target),
        event.keyspace,
        event.object,
        event.arguments);
  }

  /** Builds an instance from a {@code SCHEMA_CHANGE} response to a DDL query. */
  public static SchemaRefreshRequest fromResponse(SchemaChange response) {
    return new SchemaRefreshRequest(
        SchemaChangeType.fromProtocolString(response.changeType),
        SchemaChangeScope.fromProtocolString(response.target),
        response.keyspace,
        response.object,
        response.arguments);
  }

  public static SchemaRefreshRequest full() {
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

  @VisibleForTesting
  public SchemaRefreshRequest(
      SchemaChangeType type,
      SchemaChangeScope scope,
      String keyspace,
      String object,
      List<String> arguments) {
    this(type, scope, keyspace, object, arguments, new CompletableFuture<>());
  }

  private SchemaRefreshRequest(
      SchemaChangeType type,
      SchemaChangeScope scope,
      String keyspace,
      String object,
      List<String> arguments,
      CompletableFuture<Metadata> future) {
    this.type = type;
    this.scope = scope;
    this.keyspace = keyspace;
    this.object = object;
    this.arguments = arguments;
    this.future = future;
  }

  public SchemaRefreshRequest copy(SchemaChangeScope newScope) {
    return new SchemaRefreshRequest(type, newScope, keyspace, object, arguments, future);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof SchemaRefreshRequest) {
      SchemaRefreshRequest that = (SchemaRefreshRequest) other;
      return Objects.equals(this.type, that.type)
          && Objects.equals(this.scope, that.scope)
          && Objects.equals(this.keyspace, that.keyspace)
          && Objects.equals(this.object, that.object)
          && Objects.equals(this.arguments, that.arguments);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, scope, keyspace, object, arguments);
  }
}
