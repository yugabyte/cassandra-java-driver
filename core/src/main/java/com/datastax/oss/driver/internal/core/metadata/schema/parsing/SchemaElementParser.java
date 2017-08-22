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
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

abstract class SchemaElementParser {

  final SchemaRows rows;
  final DefaultMetadata currentMetadata;
  final InternalDriverContext context;
  final String logPrefix;
  final DataTypeParser dataTypeParser;

  SchemaElementParser(SchemaParser parent) {
    this.rows = parent.rows;
    this.currentMetadata = parent.currentMetadata;
    this.context = parent.context;
    this.logPrefix = parent.logPrefix;
    this.dataTypeParser = parent.dataTypeParser;
  }

  DataType parseDataType(
      String typeString, CqlIdentifier keyspaceId, Map<CqlIdentifier, UserDefinedType> userTypes) {
    return dataTypeParser.parse(typeString, keyspaceId, userTypes, context);
  }

  List<DataType> parseDataTypes(
      List<String> typeStrings,
      CqlIdentifier keyspaceId,
      Map<CqlIdentifier, UserDefinedType> userTypes) {
    if (typeStrings.isEmpty()) {
      return Collections.emptyList();
    } else {
      ImmutableList.Builder<DataType> builder = ImmutableList.builder();
      for (String typeString : typeStrings) {
        builder.add(parseDataType(typeString, keyspaceId, userTypes));
      }
      return builder.build();
    }
  }

  List<CqlIdentifier> toCqlIdentifiers(List<String> internalNames) {
    if (internalNames.isEmpty()) {
      return Collections.emptyList();
    } else {
      ImmutableList.Builder<CqlIdentifier> builder = ImmutableList.builder();
      for (String name : internalNames) {
        builder.add(CqlIdentifier.fromInternal(name));
      }
      return builder.build();
    }
  }
}
