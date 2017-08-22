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
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.ShallowUserDefinedType;
import com.datastax.oss.driver.internal.core.util.DirectedGraph;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A specialized parser to process a list of user types during a whole keyspace refresh.
 *
 * <p>UDTs can depend on each other, but the system table returns them in alphabetical order. In
 * order to properly build the definitions, we need to do a topological sort of the rows first, so
 * that each type is parsed after its dependencies.
 */
class TypeListParser {
  private final UserDefinedTypeParser typeParser;
  private final CqlIdentifier keyspaceId;
  private final DataTypeParser dataTypeParser;
  private final InternalDriverContext context;

  TypeListParser(SchemaParser parent, CqlIdentifier keyspaceId) {
    this.keyspaceId = keyspaceId;
    this.typeParser = new UserDefinedTypeParser(parent);
    this.dataTypeParser = parent.dataTypeParser;
    this.context = parent.context;
  }

  Map<CqlIdentifier, UserDefinedType> parse(Collection<AdminRow> typeRows) {
    if (typeRows.isEmpty()) {
      return Collections.emptyMap();
    } else {
      Map<CqlIdentifier, UserDefinedType> types = new LinkedHashMap<>();
      for (AdminRow row : topologicalSort(typeRows)) {
        UserDefinedType type = typeParser.parseRow(row, keyspaceId, types);
        types.put(type.getName(), type);
      }
      return ImmutableMap.copyOf(types);
    }
  }

  @VisibleForTesting
  Map<CqlIdentifier, UserDefinedType> parse(AdminRow... typeRows) {
    return parse(Arrays.asList(typeRows));
  }

  private List<AdminRow> topologicalSort(Collection<AdminRow> typeRows) {
    if (typeRows.size() == 1) {
      return ImmutableList.of(typeRows.iterator().next());
    } else {
      DirectedGraph<AdminRow> graph = new DirectedGraph<>(typeRows);
      for (AdminRow dependent : typeRows) {
        for (AdminRow dependency : typeRows) {
          if (dependent != dependency && dependsOn(dependent, dependency)) {
            // Edges mean "is depended upon by"; we want the types with no dependencies to come
            // first in the sort.
            graph.addEdge(dependency, dependent);
          }
        }
      }
      return graph.topologicalSort();
    }
  }

  private boolean dependsOn(AdminRow dependent, AdminRow dependency) {
    CqlIdentifier dependencyId = CqlIdentifier.fromInternal(dependency.getString("type_name"));
    for (String fieldTypeName : dependent.getListOfString("field_types")) {
      DataType fieldType = dataTypeParser.parse(fieldTypeName, keyspaceId, null, context);
      if (references(fieldType, dependencyId)) {
        return true;
      }
    }
    return false;
  }

  private boolean references(DataType dependent, CqlIdentifier dependency) {
    if (dependent instanceof ShallowUserDefinedType) {
      ShallowUserDefinedType userType = (ShallowUserDefinedType) dependent;
      return userType.getName().equals(dependency);
    } else if (dependent instanceof ListType) {
      ListType listType = (ListType) dependent;
      return references(listType.getElementType(), dependency);
    } else if (dependent instanceof SetType) {
      SetType setType = (SetType) dependent;
      return references(setType.getElementType(), dependency);
    } else if (dependent instanceof MapType) {
      MapType mapType = (MapType) dependent;
      return references(mapType.getKeyType(), dependency)
          || references(mapType.getValueType(), dependency);
    } else if (dependent instanceof TupleType) {
      TupleType tupleType = (TupleType) dependent;
      for (DataType componentType : tupleType.getComponentTypes()) {
        if (references(componentType, dependency)) {
          return true;
        }
      }
    }
    return false;
  }
}
