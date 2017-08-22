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
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.TypeRefresh;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import java.util.List;
import java.util.Map;

class UserDefinedTypeParser extends SchemaSingleRowElementParser<UserDefinedType> {

  UserDefinedTypeParser(SchemaParser parent) {
    super(parent, parent.rows.types);
  }

  @Override
  UserDefinedType parseRow(
      AdminRow row,
      CqlIdentifier keyspaceId,
      Map<CqlIdentifier, UserDefinedType> userDefinedTypes) {
    // Cassandra < 3.0:
    // CREATE TABLE system.schema_usertypes (
    //     keyspace_name text,
    //     type_name text,
    //     field_names list<text>,
    //     field_types list<text>,
    //     PRIMARY KEY (keyspace_name, type_name)
    // ) WITH CLUSTERING ORDER BY (type_name ASC)
    //
    // Cassandra >= 3.0:
    // CREATE TABLE system_schema.types (
    //     keyspace_name text,
    //     type_name text,
    //     field_names frozen<list<text>>,
    //     field_types frozen<list<text>>,
    //     PRIMARY KEY (keyspace_name, type_name)
    // ) WITH CLUSTERING ORDER BY (type_name ASC)
    CqlIdentifier name = CqlIdentifier.fromInternal(row.getString("type_name"));
    List<CqlIdentifier> fieldNames = toCqlIdentifiers(row.getListOfString("field_names"));
    List<DataType> fieldTypes =
        parseDataTypes(row.getListOfString("field_types"), keyspaceId, userDefinedTypes);

    return new DefaultUserDefinedType(keyspaceId, name, false, fieldNames, fieldTypes, context);
  }

  @Override
  MetadataRefresh newRefresh(UserDefinedType type) {
    return new TypeRefresh(currentMetadata, rows.changeType, type, logPrefix);
  }
}
