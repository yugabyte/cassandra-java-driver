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
package com.datastax.oss.driver.api.core.session;

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import java.util.concurrent.CompletionStage;

/**
 * A nexus to send requests to a Cassandra cluster.
 *
 * <p>For CQL, it exposes simple methods that take {@link Statement} instances (such a {@link
 * #execute(Statement)}, {@link #executeAsync(Statement)}, etc).
 *
 * <p>At a higher level, it is able to handle arbitrary request and result types ({@link
 * #execute(Request, GenericType)}), that will be processed by plugins to the driver's request
 * execution logic. This is intended for future extensions, for example a reactive API for CQL
 * statements, or graph requests in the Datastax Enterprise driver. For more details on custom
 * request processors, refer to the manual or the sources (in particular {@code RequestProcessor}).
 */
public interface Session extends AsyncAutoCloseable {

  /**
   * The keyspace that this session is currently connected to.
   *
   * <p>There are two ways that this can be set:
   *
   * <ul>
   *   <li>during initialization, if the session was created with {@link
   *       Cluster#connect(CqlIdentifier)} or {@link Cluster#connectAsync(CqlIdentifier)};
   *   <li>at runtime, if the client issues a request that changes the keyspace (such as a CQL
   *       {@code USE} query). Note that this second method is inherently unsafe, since other
   *       requests expecting the old keyspace might be executing concurrently. Therefore it is
   *       highly discouraged, aside from trivial cases (such as a cqlsh-style program where
   *       requests are never concurrent).
   * </ul>
   */
  CqlIdentifier getKeyspace();

  /**
   * Executes an arbitrary request.
   *
   * @param resultType the type of the result, which determines the internal request processor
   *     (built-in or custom) that will be used to handle the request.
   * @see Session
   */
  <RequestT extends Request, ResultT> ResultT execute(
      RequestT request, GenericType<ResultT> resultType);

  /**
   * Executes a CQL statement synchronously (the calling thread blocks until the result becomes
   * available).
   */
  default ResultSet execute(Statement<?> statement) {
    return execute(statement, Statement.SYNC);
  }

  /**
   * Executes a CQL statement synchronously (the calling thread blocks until the result becomes
   * available).
   */
  default ResultSet execute(String query) {
    return execute(SimpleStatement.newInstance(query));
  }

  /**
   * Executes a CQL statement asynchronously (the call returns as soon as the statement was sent,
   * generally before the result is available).
   */
  default CompletionStage<AsyncResultSet> executeAsync(Statement<?> statement) {
    return execute(statement, Statement.ASYNC);
  }

  /**
   * Executes a CQL statement asynchronously (the call returns as soon as the statement was sent,
   * generally before the result is available).
   */
  default CompletionStage<AsyncResultSet> executeAsync(String query) {
    return executeAsync(SimpleStatement.newInstance(query));
  }

  /**
   * Prepares a CQL statement synchronously (the calling thread blocks until the statement is
   * prepared).
   */
  default PreparedStatement prepare(SimpleStatement query) {
    return execute(new DefaultPrepareRequest(query), PrepareRequest.SYNC);
  }

  /**
   * Prepares a CQL statement synchronously (the calling thread blocks until the statement is
   * prepared).
   */
  default PreparedStatement prepare(String query) {
    return execute(new DefaultPrepareRequest(query), PrepareRequest.SYNC);
  }

  /**
   * Prepares a CQL statement asynchronously (the call returns as soon as the prepare query was
   * sent, generally before the statement is prepared).
   */
  default CompletionStage<PreparedStatement> prepareAsync(String query) {
    return execute(new DefaultPrepareRequest(query), PrepareRequest.ASYNC);
  }

  /**
   * Prepares a CQL statement asynchronously (the call returns as soon as the prepare query was
   * sent, generally before the statement is prepared).
   */
  default CompletionStage<PreparedStatement> prepareAsync(SimpleStatement query) {
    return execute(new DefaultPrepareRequest(query), PrepareRequest.ASYNC);
  }
}
