package com.datastax.oss.driver.internal.core.metadata.schema.refresh;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;

abstract class KeyspaceRefreshTestBase {
  protected static final UserDefinedType OLD_T1 =
      new UserDefinedTypeBuilder(
              CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("t1"))
          .withField(CqlIdentifier.fromInternal("i"), DataTypes.INT)
          .build();
  protected static final UserDefinedType OLD_T2 =
      new UserDefinedTypeBuilder(
              CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("t2"))
          .withField(CqlIdentifier.fromInternal("i"), DataTypes.INT)
          .build();
  protected static final DefaultKeyspaceMetadata OLD_KS1 = newKeyspace("ks1", true, OLD_T1, OLD_T2);

  protected DefaultMetadata oldMetadata =
      DefaultMetadata.EMPTY.withKeyspaces(ImmutableMap.of(OLD_KS1.getName(), OLD_KS1));

  protected static DefaultKeyspaceMetadata newKeyspace(
      String name, boolean durableWrites, UserDefinedType... userTypes) {
    ImmutableMap.Builder<CqlIdentifier, UserDefinedType> typesMapBuilder = ImmutableMap.builder();
    for (UserDefinedType type : userTypes) {
      typesMapBuilder.put(type.getName(), type);
    }
    return new DefaultKeyspaceMetadata(
        CqlIdentifier.fromInternal(name),
        durableWrites,
        Collections.emptyMap(),
        typesMapBuilder.build(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap());
  }
}
