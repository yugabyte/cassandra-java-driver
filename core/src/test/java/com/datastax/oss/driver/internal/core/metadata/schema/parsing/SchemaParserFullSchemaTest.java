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
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeScope;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.FullSchemaRefresh;
import com.google.common.collect.ImmutableList;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class SchemaParserFullSchemaTest extends SchemaParserTest {

  @Test
  public void should_parse_keyspaces() {
    FullSchemaRefresh refresh =
        (FullSchemaRefresh)
            parse(
                rows ->
                    rows.withKeyspaces(
                            ImmutableList.of(
                                mockModernKeyspaceRow("ks1"), mockModernKeyspaceRow("ks2")))
                        .withTypes(
                            ImmutableList.of(
                                mockTypeRow(
                                    "ks1", "t1", ImmutableList.of("i"), ImmutableList.of("int")),
                                mockTypeRow(
                                    "ks2", "t2", ImmutableList.of("i"), ImmutableList.of("int")))));

    Map<CqlIdentifier, KeyspaceMetadata> keyspaces = refresh.newKeyspaces;
    assertThat(keyspaces).hasSize(2);
    KeyspaceMetadata ks1 = keyspaces.get(CqlIdentifier.fromInternal("ks1"));
    KeyspaceMetadata ks2 = keyspaces.get(CqlIdentifier.fromInternal("ks2"));

    assertThat(ks1.getName().asInternal()).isEqualTo("ks1");
    assertThat(ks1.getUserDefinedTypes()).hasSize(1).containsKey(CqlIdentifier.fromInternal("t1"));
    assertThat(ks2.getName().asInternal()).isEqualTo("ks2");
    assertThat(ks2.getUserDefinedTypes()).hasSize(1).containsKey(CqlIdentifier.fromInternal("t2"));
  }

  private MetadataRefresh parse(Consumer<SchemaRows.Builder> builderConfig) {
    SchemaRows.Builder builder =
        new SchemaRows.Builder(
            node, SchemaChangeType.UPDATED, SchemaChangeScope.FULL_SCHEMA, "table_name", "test");
    builderConfig.accept(builder);
    SchemaRows rows = builder.build();
    return new SchemaParser(rows, currentMetadata, context, "test").parse();
  }
}
