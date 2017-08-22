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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class DefaultKeyspaceMetadata implements KeyspaceMetadata {

  private final CqlIdentifier name;
  private final boolean durableWrites;
  private final Map<String, String> replicationOptions;
  private final Map<CqlIdentifier, UserDefinedType> types;
  private final ImmutableMap<CqlIdentifier, TableMetadata> tables;
  private final ImmutableMap<CqlIdentifier, ViewMetadata> views;
  private final ImmutableMap<FunctionSignature, FunctionMetadata> functions;
  private final ImmutableMap<FunctionSignature, AggregateMetadata> aggregates;

  public DefaultKeyspaceMetadata(
      CqlIdentifier name,
      boolean durableWrites,
      Map<String, String> replicationOptions,
      Map<CqlIdentifier, UserDefinedType> types,
      ImmutableMap<CqlIdentifier, TableMetadata> tables,
      ImmutableMap<CqlIdentifier, ViewMetadata> views,
      ImmutableMap<FunctionSignature, FunctionMetadata> functions,
      ImmutableMap<FunctionSignature, AggregateMetadata> aggregates) {
    this.name = name;
    this.durableWrites = durableWrites;
    this.replicationOptions = replicationOptions;
    this.types = types;
    this.tables = tables;
    this.views = views;
    this.functions = functions;
    this.aggregates = aggregates;
  }

  @Override
  public CqlIdentifier getName() {
    return name;
  }

  @Override
  public boolean isDurableWrites() {
    return durableWrites;
  }

  @Override
  public Map<String, String> getReplication() {
    return replicationOptions;
  }

  @Override
  public Map<CqlIdentifier, UserDefinedType> getUserDefinedTypes() {
    return types;
  }

  @Override
  public ImmutableMap<CqlIdentifier, TableMetadata> getTables() {
    return tables;
  }

  @Override
  public ImmutableMap<CqlIdentifier, ViewMetadata> getViews() {
    return views;
  }

  @Override
  public ImmutableMap<FunctionSignature, FunctionMetadata> getFunctions() {
    return functions;
  }

  @Override
  public ImmutableMap<FunctionSignature, AggregateMetadata> getAggregates() {
    return aggregates;
  }
}
