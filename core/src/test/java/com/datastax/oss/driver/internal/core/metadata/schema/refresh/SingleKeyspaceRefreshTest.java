package com.datastax.oss.driver.internal.core.metadata.schema.refresh;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.SchemaChangeType;
import com.datastax.oss.driver.internal.core.metadata.schema.events.KeyspaceChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TypeChangeEvent;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SingleKeyspaceRefreshTest extends KeyspaceRefreshTestBase {

  @Test
  public void should_detect_dropped_keyspace() {
    SingleKeyspaceRefresh refresh = SingleKeyspaceRefresh.dropped(oldMetadata, "ks1", "test");
    refresh.compute();
    assertThat(refresh.newMetadata.getKeyspaces()).isEmpty();
    assertThat(refresh.events).containsExactly(KeyspaceChangeEvent.dropped(OLD_KS1));
  }

  @Test
  public void should_ignore_dropped_keyspace_if_unknown() {
    SingleKeyspaceRefresh refresh = SingleKeyspaceRefresh.dropped(oldMetadata, "ks2", "test");
    refresh.compute();
    assertThat(refresh.newMetadata.getKeyspaces()).hasSize(1);
    assertThat(refresh.events).isEmpty();
  }

  @Test
  public void should_detect_created_keyspace() {
    DefaultKeyspaceMetadata ks2 = newKeyspace("ks2", true);
    SingleKeyspaceRefresh refresh =
        SingleKeyspaceRefresh.createdOrUpdated(oldMetadata, SchemaChangeType.CREATED, ks2, "test");
    refresh.compute();
    assertThat(refresh.newMetadata.getKeyspaces()).hasSize(2);
    assertThat(refresh.events).containsExactly(KeyspaceChangeEvent.created(ks2));
  }

  @Test
  public void should_detect_top_level_update_in_keyspace() {
    // Change only one top-level option (durable writes)
    DefaultKeyspaceMetadata newKs1 = newKeyspace("ks1", false, OLD_T1, OLD_T2);
    SingleKeyspaceRefresh refresh =
        SingleKeyspaceRefresh.createdOrUpdated(oldMetadata, SchemaChangeType.UPDATED, newKs1, "test");
    refresh.compute();
    assertThat(refresh.newMetadata.getKeyspaces()).hasSize(1);
    assertThat(refresh.events).containsExactly(KeyspaceChangeEvent.updated(OLD_KS1, newKs1));
  }

  @Test
  public void should_detect_updated_children_in_keyspace() {
    // Drop one type, modify the other and add a third one
    UserDefinedType newT2 =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("t2"))
            .withField(CqlIdentifier.fromInternal("i"), DataTypes.TEXT)
            .build();
    UserDefinedType t3 =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("t3"))
            .withField(CqlIdentifier.fromInternal("i"), DataTypes.INT)
            .build();
    DefaultKeyspaceMetadata newKs1 = newKeyspace("ks1", true, newT2, t3);

    SingleKeyspaceRefresh refresh =
        SingleKeyspaceRefresh.createdOrUpdated(oldMetadata, SchemaChangeType.UPDATED, newKs1, "test");
    refresh.compute();
    assertThat(refresh.newMetadata.getKeyspaces().get(OLD_KS1.getName())).isEqualTo(newKs1);
    assertThat(refresh.events)
        .containsExactly(
            TypeChangeEvent.dropped(OLD_T1),
            TypeChangeEvent.updated(OLD_T2, newT2),
            TypeChangeEvent.created(t3));
  }

  @Test
  public void should_detect_top_level_change_and_children_changes() {
    // Drop one type, modify the other and add a third one
    UserDefinedType newT2 =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("t2"))
            .withField(CqlIdentifier.fromInternal("i"), DataTypes.TEXT)
            .build();
    UserDefinedType t3 =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("t3"))
            .withField(CqlIdentifier.fromInternal("i"), DataTypes.INT)
            .build();
    // Also disable durable writes
    DefaultKeyspaceMetadata newKs1 = newKeyspace("ks1", false, newT2, t3);

    SingleKeyspaceRefresh refresh =
        SingleKeyspaceRefresh.createdOrUpdated(oldMetadata, SchemaChangeType.UPDATED, newKs1, "test");
    refresh.compute();
    assertThat(refresh.newMetadata.getKeyspaces().get(OLD_KS1.getName())).isEqualTo(newKs1);
    assertThat(refresh.events)
        .containsExactly(
            KeyspaceChangeEvent.updated(OLD_KS1, newKs1),
            TypeChangeEvent.dropped(OLD_T1),
            TypeChangeEvent.updated(OLD_T2, newT2),
            TypeChangeEvent.created(t3));
  }
}
