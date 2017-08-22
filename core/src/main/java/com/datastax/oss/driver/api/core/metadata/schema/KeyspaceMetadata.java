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
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import com.google.common.collect.Iterables;
import java.util.Map;

/** A keyspace in the schema metadata. */
public interface KeyspaceMetadata extends Describable {
  CqlIdentifier getName();

  /** Whether durable writes are set on this keyspace. */
  boolean isDurableWrites();

  /** The replication options defined for this keyspace. */
  Map<String, String> getReplication();

  Map<CqlIdentifier, TableMetadata> getTables();

  Map<CqlIdentifier, ViewMetadata> getViews();

  Map<CqlIdentifier, UserDefinedType> getUserDefinedTypes();

  Map<FunctionSignature, FunctionMetadata> getFunctions();

  Map<FunctionSignature, AggregateMetadata> getAggregates();

  @Override
  default String describe(boolean pretty) {
    ScriptBuilder builder =
        new ScriptBuilder(pretty)
            .append("CREATE KEYSPACE ")
            .append(getName())
            .append(" WITH replication = { 'class' : '")
            .append(getReplication().get("class"))
            .append("'");
    for (Map.Entry<String, String> entry : getReplication().entrySet()) {
      if (!entry.getKey().equals("class")) {
        builder
            .append(", '")
            .append(entry.getKey())
            .append("': '")
            .append(entry.getValue())
            .append("'");
      }
    }
    return builder
        .append(" } AND durable_writes = ")
        .append(Boolean.toString(isDurableWrites()))
        .append(";")
        .build();
  }

  @Override
  default String describeWithChildren(boolean pretty) {
    String createKeyspace = describe(pretty);
    ScriptBuilder builder = new ScriptBuilder(pretty).append(createKeyspace);

    for (Describable element :
        Iterables.concat(
            getUserDefinedTypes().values(),
            getTables().values(),
            getViews().values(),
            getFunctions().values(),
            getAggregates().values())) {
      builder.forceNewLine(2).append(element.describeWithChildren(pretty));
    }

    return builder.build();
  }
}
