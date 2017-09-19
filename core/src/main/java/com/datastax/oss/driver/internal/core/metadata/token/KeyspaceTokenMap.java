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
package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.util.NanoTime;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The token data for a given replication configuration. It's shared by all keyspaces that use that
 * configuration.
 */
class KeyspaceTokenMap {

  private static final Logger LOG = LoggerFactory.getLogger(KeyspaceTokenMap.class);

  static KeyspaceTokenMap build(
      Map<String, String> replicationConfig,
      Map<Token, Node> tokenToPrimary,
      List<Token> ring,
      Set<TokenRange> tokenRanges,
      TokenFactory tokenFactory,
      String logPrefix) {

    long start = System.nanoTime();
    try {
      ReplicationStrategy strategy = ReplicationStrategy.newInstance(replicationConfig, logPrefix);

      Map<Token, Set<Node>> replicasByToken = strategy.computeReplicasByToken(tokenToPrimary, ring);
      Map<Node, Set<TokenRange>> tokenRangesByNode;
      if (ring.size() == 1) {
        // We forced the single range to ]minToken,minToken], make sure to use that instead of relying
        // on the node's token
        ImmutableMap.Builder<Node, Set<TokenRange>> builder = ImmutableMap.builder();
        for (Node node : tokenToPrimary.values()) {
          builder.put(node, tokenRanges);
        }
        tokenRangesByNode = builder.build();
      } else {
        tokenRangesByNode = buildTokenRangesByNode(tokenRanges, replicasByToken);
      }
      return new KeyspaceTokenMap(ring, tokenRangesByNode, replicasByToken, tokenFactory);
    } finally {
      LOG.trace(
          "[{}] Computing keyspace-level data for {} took {}",
          logPrefix,
          replicationConfig,
          NanoTime.formatTimeSince(start));
    }
  }

  private final List<Token> ring;
  private final Map<Node, Set<TokenRange>> tokenRangesByNode;
  private final Map<Token, Set<Node>> replicasByToken;
  private final TokenFactory tokenFactory;

  private KeyspaceTokenMap(
      List<Token> ring,
      Map<Node, Set<TokenRange>> tokenRangesByNode,
      Map<Token, Set<Node>> replicasByToken,
      TokenFactory tokenFactory) {
    this.ring = ring;
    this.tokenRangesByNode = tokenRangesByNode;
    this.replicasByToken = replicasByToken;
    this.tokenFactory = tokenFactory;
  }

  Set<TokenRange> getTokenRanges(Node replica) {
    return tokenRangesByNode.getOrDefault(replica, Collections.emptySet());
  }

  Set<Node> getReplicas(ByteBuffer partitionKey) {
    return getReplicas(tokenFactory.hash(partitionKey));
  }

  Set<Node> getReplicas(TokenRange range) {
    return getReplicas(range.getEnd());
  }

  private Set<Node> getReplicas(Token token) {
    // If the token happens to be one of the "primary" tokens, get result directly
    Set<Node> nodes = replicasByToken.get(token);
    if (nodes != null) {
      return nodes;
    }
    // Otherwise, find closest "primary" token on the ring
    int i = Collections.binarySearch(ring, token);
    if (i < 0) {
      i = -i - 1;
      if (i >= ring.size()) {
        i = 0;
      }
    }
    return replicasByToken.get(ring.get(i));
  }

  private static Map<Node, Set<TokenRange>> buildTokenRangesByNode(
      Set<TokenRange> tokenRanges, Map<Token, Set<Node>> replicasByToken) {
    Map<Node, ImmutableSet.Builder<TokenRange>> builders = new HashMap<>();
    for (TokenRange range : tokenRanges) {
      Set<Node> replicas = replicasByToken.get(range.getEnd());
      for (Node node : replicas) {
        ImmutableSet.Builder<TokenRange> nodeRanges =
            builders.computeIfAbsent(node, k -> ImmutableSet.builder());
        nodeRanges.add(range);
      }
    }
    ImmutableMap.Builder<Node, Set<TokenRange>> result = ImmutableMap.builder();
    for (Map.Entry<Node, ImmutableSet.Builder<TokenRange>> entry : builders.entrySet()) {
      result.put(entry.getKey(), entry.getValue().build());
    }
    return result.build();
  }
}
