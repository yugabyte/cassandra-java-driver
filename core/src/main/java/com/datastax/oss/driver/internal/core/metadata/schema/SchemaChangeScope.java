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

import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** The scope of a schema change. */
public enum SchemaChangeScope {
  // Note that FULL_SCHEMA and VIEW are not defined at the protocol level, they're only used
  // internally.
  FULL_SCHEMA("FULL_SCHEMA"),
  KEYSPACE(ProtocolConstants.SchemaChangeTarget.KEYSPACE),
  TABLE(ProtocolConstants.SchemaChangeTarget.TABLE),
  VIEW("VIEW"),
  TYPE(ProtocolConstants.SchemaChangeTarget.TYPE),
  FUNCTION(ProtocolConstants.SchemaChangeTarget.FUNCTION),
  AGGREGATE(ProtocolConstants.SchemaChangeTarget.AGGREGATE),
  ;

  private final String protocolString;

  SchemaChangeScope(String protocolString) {
    this.protocolString = protocolString;
  }

  private static final Map<String, SchemaChangeScope> BY_PROTOCOL_STRING;

  static {
    ImmutableMap.Builder<String, SchemaChangeScope> builder = ImmutableMap.builder();
    for (SchemaChangeScope scope : values()) {
      builder.put(scope.protocolString, scope);
    }
    BY_PROTOCOL_STRING = builder.build();
  }

  public static SchemaChangeScope fromProtocolString(String protocolString) {
    SchemaChangeScope scope = BY_PROTOCOL_STRING.get(protocolString);
    if (scope == null) {
      throw new IllegalArgumentException("Unsupported schema type: " + protocolString);
    }
    return scope;
  }
}
