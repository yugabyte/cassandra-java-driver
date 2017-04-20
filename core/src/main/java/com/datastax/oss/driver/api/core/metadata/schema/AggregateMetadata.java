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
package com.datastax.oss.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;

/** A CQL aggregate in the schema metadata. */
public interface AggregateMetadata extends Describable {
  KeyspaceMetadata getKeyspace();

  FunctionSignature getSignature();

  /**
   * The final function of this aggregate, or {@code null} if there is none.
   *
   * <p>This is the function specified with {@code FINALFUNC} in the {@code CREATE AGGREGATE...}
   * statement. It transforms the final value after the aggregation is complete.
   */
  FunctionMetadata getFinalFunc();

  /**
   * The initial state value of this aggregate, or {@code null} if there is none.
   *
   * <p>This is the value specified with {@code INITCOND} in the {@code CREATE AGGREGATE...}
   * statement. It's passed to the initial invocation of the state function (if that function does
   * not accept null arguments).
   *
   * <p>The actual type of the returned object depends on the aggregate's {@link #getStateType()
   * state type} and on the {@link TypeCodec codec} used to {@link TypeCodec#parse(String) parse}
   * the {@code INITCOND} literal.
   *
   * <p>If, for some reason, the {@code INITCOND} literal cannot be parsed, a warning will be logged
   * and the returned object will be the original {@code INITCOND} literal in its textual,
   * non-parsed form.
   *
   * @return the initial state, or {@code null} if there is none.
   */
  Object getInitCond();

  /**
   * The return type of this aggregate.
   *
   * <p>This is the final type of the value computed by this aggregate; in other words, the return
   * type of the final function if it is defined, or the state type otherwise.
   */
  DataType getReturnType();

  /**
   * The state function of this aggregate.
   *
   * <p>This is the function specified with {@code SFUNC} in the {@code CREATE AGGREGATE...}
   * statement. It aggregates the current state with each row to produce a new state.
   */
  FunctionMetadata getStateFunc();

  /**
   * The state type of this aggregate.
   *
   * <p>This is the type specified with {@code STYPE} in the {@code CREATE AGGREGATE...} statement.
   * It defines the type of the value that is accumulated as the aggregate iterates through the
   * rows.
   */
  DataType getStateType();
}
