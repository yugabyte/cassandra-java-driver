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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import java.util.Map;

/** A keyspace in the schema metadata. */
public interface KeyspaceMetadata extends Describable {
  CqlIdentifier getName();

  /** Whether durable writes are set on this keyspace. */
  boolean isDurableWrites();

  /** The replication options defined for this keyspace. */
  Map<String, String> getReplication();

  Map<CqlIdentifier, TableMetadata> getTables();

  Map<CqlIdentifier, MaterializedViewMetadata> getMaterializedViews();

  Map<CqlIdentifier, UserDefinedType> getUserDefinedTypes();

  Map<FunctionSignature, FunctionMetadata> getFunctions();

  Map<FunctionSignature, AggregateMetadata> getAggregates();
}
