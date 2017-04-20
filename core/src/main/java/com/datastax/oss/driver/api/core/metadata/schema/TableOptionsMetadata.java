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

import java.nio.ByteBuffer;
import java.util.Map;

/** The options of a table or materialized view in the schema metadata. */
public interface TableOptionsMetadata {

  /** Whether the table uses the {@code COMPACT STORAGE} option. */
  boolean isCompactStorage();

  /** The commentary set for this table, or {@code null} if none has been set. */
  String getComment();

  /** The chance with which a read repair is triggered for this table (in [0.0, 1.0]). */
  Double getReadRepairChance();

  /** The chance with which a local read repair is triggered for this table (in [0.0, 1.0]). */
  Double getLocalReadRepairChance();

  /** The tombstone garbage collection grace time in seconds for this table. */
  Integer getGcGraceInSeconds();

  /** The false positive chance for the Bloom filter of this table (in [0.0, 1.0]). */
  Double getBloomFilterFalsePositiveChance();

  /** The caching options for this table. */
  Map<String, String> getCaching();

  /** Whether the populate I/O cache on flush is set on this table. */
  Boolean getPopulateIOCacheOnFlush();

  /** The memtable flush period (in milliseconds), or 0 if no periodic flush is configured. */
  Integer getMemtableFlushPeriodInMs();

  /** The default TTL, or 0 if no default TTL is configured. */
  Integer getDefaultTimeToLive();

  /** The speculative retry strategy. */
  String getSpeculativeRetry();

  /** The minimum index interval. */
  Integer getMinIndexInterval();

  /** The maximum index interval. */
  Integer getMaxIndexInterval();

  /**
   * When compression is enabled, the probability with which checksums for compressed blocks are
   * checked during reads.
   *
   * <p>Note that this option is available in Cassandra 3.0.0 and above, when it became a
   * "top-level" table option, whereas previously it was a suboption of the {@link #getCompression()
   * compression} option.
   *
   * <p>For Cassandra versions prior to 3.0.0, this method always returns {@code null}.
   */
  Double getCrcCheckChance();

  /** The compaction options. */
  Map<String, String> getCompaction();

  /** The compression options. */
  Map<String, String> getCompression();

  /**
   * The extension options.
   *
   * <p>For Cassandra versions prior to 3.0.0, this method always returns an empty map.
   */
  Map<String, ByteBuffer> getExtensions();

  /**
   * Whether or not change data capture is enabled.
   *
   * <p>For Cassandra versions prior to 3.8.0, this method always returns false.
   */
  Boolean isCDC();
}
