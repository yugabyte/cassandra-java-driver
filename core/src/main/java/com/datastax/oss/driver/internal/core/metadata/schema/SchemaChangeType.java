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

public enum SchemaChangeType {
  CREATED(ProtocolConstants.SchemaChangeType.CREATED),
  UPDATED(ProtocolConstants.SchemaChangeType.UPDATED),
  DROPPED(ProtocolConstants.SchemaChangeType.DROPPED),
  ;

  private final String protocolString;

  SchemaChangeType(String protocolString) {
    this.protocolString = protocolString;
  }

  private static final Map<String, SchemaChangeType> BY_PROTOCOL_STRING;

  static {
    ImmutableMap.Builder<String, SchemaChangeType> builder = ImmutableMap.builder();
    for (SchemaChangeType type : values()) {
      builder.put(type.protocolString, type);
    }
    BY_PROTOCOL_STRING = builder.build();
  }

  public static SchemaChangeType fromProtocolString(String protocolString) {
    SchemaChangeType type = BY_PROTOCOL_STRING.get(protocolString);
    if (type == null) {
      throw new IllegalArgumentException("Unsupported schema change type: " + protocolString);
    }
    return type;
  }
}
