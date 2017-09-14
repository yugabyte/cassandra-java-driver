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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.SchemaParser;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaQueries;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Holds the immutable instance of the {@link Metadata}, and handles requests to update it. */
public class MetadataManager implements AsyncAutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);

  private final InternalDriverContext context;
  private final String logPrefix;
  private final EventExecutor adminExecutor;
  private final SingleThreaded singleThreaded;
  private final ControlConnection controlConnection;
  private volatile DefaultMetadata metadata; // must be updated on adminExecutor only

  public MetadataManager(InternalDriverContext context) {
    this.context = context;
    this.logPrefix = context.clusterName();
    this.adminExecutor = context.nettyOptions().adminEventExecutorGroup().next();
    this.singleThreaded = new SingleThreaded();
    this.controlConnection = context.controlConnection();
    this.metadata = DefaultMetadata.EMPTY;
  }

  public Metadata getMetadata() {
    return this.metadata;
  }

  public CompletionStage<Void> addContactPoints(Set<InetSocketAddress> contactPoints) {
    if (contactPoints == null || contactPoints.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    } else {
      LOG.debug("[{}] Adding initial contact points {}", logPrefix, contactPoints);
      CompletableFuture<Void> initNodesFuture = new CompletableFuture<>();
      RunOrSchedule.on(
          adminExecutor, () -> singleThreaded.initNodes(contactPoints, initNodesFuture));
      return initNodesFuture;
    }
  }

  public CompletionStage<Void> refreshNodes() {
    return context
        .topologyMonitor()
        .refreshNodeList()
        .thenApplyAsync(singleThreaded::refreshNodes, adminExecutor);
  }

  public CompletionStage<Void> refreshNode(Node node) {
    return context
        .topologyMonitor()
        .refreshNode(node)
        // The callback only updates volatile fields so no need to schedule it on adminExecutor
        .thenApply(
            maybeInfo -> {
              if (maybeInfo.isPresent()) {
                NodesRefresh.copyInfos(maybeInfo.get(), (DefaultNode) node, logPrefix);
              } else {
                LOG.debug(
                    "[{}] Topology monitor did not return any info for the refresh of {}, skipping",
                    logPrefix,
                    node);
              }
              return null;
            });
  }

  public void addNode(InetSocketAddress address) {
    context
        .topologyMonitor()
        .getNewNodeInfo(address)
        .whenCompleteAsync(
            (info, error) -> {
              if (error != null) {
                LOG.debug(
                    "[{}] Error refreshing node info for {}, "
                        + "this will be retried on the next full refresh",
                    logPrefix,
                    address,
                    error);
              } else {
                singleThreaded.addNode(address, info);
              }
            },
            adminExecutor);
  }

  public void removeNode(InetSocketAddress address) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.removeNode(address));
  }

  public CompletionStage<Metadata> refreshSchema() {

    // TODO check if metadata disabled in config
    // TODO debounce
    // TODO create an event to force (even if disabled in config)
    // TODO join current update if already in progress?

    CompletionStage<Metadata> refreshFuture = new CompletableFuture<>();
    maybeInitControlConnection()
        // 1. Query system tables
        .thenCompose(v -> SchemaQueries.newInstance(refreshFuture, context).execute())
        // 2. Parse the rows into metadata objects, put them in a MetadataRefresh
        // 3. Apply the MetadataRefresh
        .thenApplyAsync(singleThreaded::refreshSchema, adminExecutor)
        .whenComplete(
            (v, error) -> {
              if (error != null) {
                LOG.warn(
                    "[{}] Unexpected error while refreshing schema, skipping", logPrefix, error);
              }
              singleThreaded.firstSchemaRefreshFuture.complete(null);
            });
    return refreshFuture;
  }

  // The control connection may or may not have been initialized already by TopologyMonitor.
  private CompletionStage<Void> maybeInitControlConnection() {
    return singleThreaded.firstSchemaRefreshFuture.isDone()
        // Not the first schema refresh, so we know init was attempted already
        ? singleThreaded.firstSchemaRefreshFuture
        : controlConnection.init(false, true);
  }

  /**
   * Returns a future that completes after the first schema refresh attempt, whether that attempt
   * succeeded or not (we wait for that refresh at init, but if it fails it's not fatal).
   */
  public CompletionStage<Void> firstSchemaRefreshFuture() {
    return singleThreaded.firstSchemaRefreshFuture;
  }

  // TODO user-controlled refresh?

  @Override
  public CompletionStage<Void> closeFuture() {
    return singleThreaded.closeFuture;
  }

  @Override
  public CompletionStage<Void> closeAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::close);
    return singleThreaded.closeFuture;
  }

  @Override
  public CompletionStage<Void> forceCloseAsync() {
    return this.closeAsync();
  }

  private class SingleThreaded {
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean closeWasCalled;
    private final CompletableFuture<Void> firstSchemaRefreshFuture = new CompletableFuture<>();

    private void initNodes(
        Set<InetSocketAddress> addresses, CompletableFuture<Void> initNodesFuture) {
      refresh(new InitContactPointsRefresh(addresses, logPrefix));
      initNodesFuture.complete(null);
    }

    private Void refreshNodes(Iterable<NodeInfo> nodeInfos) {
      return refresh(new FullNodeListRefresh(nodeInfos, logPrefix));
    }

    private void addNode(InetSocketAddress address, Optional<NodeInfo> maybeInfo) {
      try {
        if (maybeInfo.isPresent()) {
          NodeInfo info = maybeInfo.get();
          if (!address.equals(info.getConnectAddress())) {
            // This would be a bug in the TopologyMonitor, protect against it
            LOG.warn(
                "[{}] Received a request to add a node for {}, "
                    + "but the provided info uses the connect address {}, ignoring it",
                logPrefix,
                address,
                info.getConnectAddress());
          } else {
            refresh(new AddNodeRefresh(info, logPrefix));
          }
        } else {
          LOG.debug(
              "[{}] Ignoring node addition for {} because the "
                  + "topology monitor didn't return any information",
              logPrefix,
              address);
        }
      } catch (Throwable t) {
        LOG.warn("[" + logPrefix + "] Unexpected exception while handling added node", logPrefix);
      }
    }

    private void removeNode(InetSocketAddress address) {
      refresh(new RemoveNodeRefresh(address, logPrefix));
    }

    private Void refreshSchema(SchemaRows schemaRows) {
      assert adminExecutor.inEventLoop();
      MetadataRefresh schemaRefresh = new SchemaParser(schemaRows, context).parse();
      if (schemaRefresh != null) {
        refresh(schemaRefresh);
      }
      return null;
    }

    private void close() {
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;
      LOG.debug("[{}] Closing", logPrefix);
      closeFuture.complete(null);
    }
  }

  @VisibleForTesting
  Void refresh(MetadataRefresh refresh) {
    assert adminExecutor.inEventLoop();
    if (!singleThreaded.closeWasCalled) {
      MetadataRefresh.Result result = refresh.compute(metadata);
      metadata = result.newMetadata;
      for (Object event : result.events) {
        context.eventBus().fire(event);
      }
    }
    return null;
  }
}
