// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
package com.yugabyte.driver.core.policies;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.policies.ChainableLoadBalancingPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.yugabyte.driver.core.PartitionMetadata;
import com.yugabyte.driver.core.TableSplitMetadata;
import com.yugabyte.driver.core.utils.Jenkins;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The load-balancing policy to direct a statement to the hosts where the tablet for the partition
 * key resides, with the tablet leader host at the beginning of the host list as the preferred host.
 * The hosts are found by computing the hash key and looking them up in the partition metadata from
 * the system.partitions table.
 *
 * @see PartitionMetadata
 */
public class PartitionAwarePolicy implements ChainableLoadBalancingPolicy {

  private final LoadBalancingPolicy childPolicy;
  private volatile Metadata clusterMetadata;
  private Configuration configuration;

  private static final Logger logger = LoggerFactory.getLogger(PartitionAwarePolicy.class);

  /**
   * Creates a new {@code PartitionAware} policy.
   *
   * @param childPolicy the load balancing policy to wrap with partition awareness
   */
  public PartitionAwarePolicy(LoadBalancingPolicy childPolicy) {
    this.childPolicy = childPolicy;
  }

  /** Creates a new {@code PartitionAware} policy with additional default data-center awareness. */
  public PartitionAwarePolicy() {
    this(new DCAwareRoundRobinPolicy.Builder().withUsedHostsPerRemoteDc(Integer.MAX_VALUE).build());
  }

  @Override
  public void init(Cluster cluster, Collection<Host> hosts) {
    clusterMetadata = cluster.getMetadata();
    configuration = cluster.getConfiguration();
    childPolicy.init(cluster, hosts);
  }

  @Override
  public LoadBalancingPolicy getChildPolicy() {
    return childPolicy;
  }

  /**
   * Returns the hash key for the given bytes. The hash key is an unsigned 16-bit number.
   *
   * @param bytes the bytes to calculate the hash key
   * @return the hash key
   */
  private static int getKey(byte bytes[]) {
    final long SEED = 97;
    long h = Jenkins.hash64(bytes, SEED);
    long h1 = h >>> 48;
    long h2 = 3 * (h >>> 32);
    long h3 = 5 * (h >>> 16);
    long h4 = 7 * (h & 0xffff);
    return (int) ((h1 ^ h2 ^ h3 ^ h4) & 0xffff);
  }

  /**
   * Internally we use a unsigned 16-bit hash CQL uses signed 64-bit. This method converts from the
   * user-visible (e.g. via 'token') CQL hash to our internal representation used for partitioning.
   *
   * @param cql_hash the CQL hash key
   * @return the corresponding internal YB hash key
   */
  public static int CqlToYBHashCode(long cql_hash) {
    int hash_code = (int) (cql_hash >> 48);
    hash_code ^= 0x8000; // flip first bit so that negative values are smaller than positives.
    return hash_code;
  }

  /**
   * Internally we use a unsigned 16-bit hash CQL uses signed 64-bit. This method converts from our
   * internal representation used for partitioning to the user-visible (e.g. via 'token') CQL hash.
   *
   * @param hash the internal YB hash key
   * @return a corresponding CQL hash key
   */
  public static long YBToCqlHashCode(int hash) {
    long cql_hash = hash ^ 0x8000; // undo the flipped bit
    cql_hash = cql_hash << 48;
    return cql_hash;
  }

  /**
   * Returns the hash key for the given bound statement. The hash key can be determined only for
   * DMLs and when the partition key is specified in the bind variables.
   *
   * @param stmt the statement to calculate the hash key for
   * @return the hash key for the statement, or -1 when hash key cannot be determined
   */
  static int getKey(BoundStatement stmt) {
    PreparedStatement pstmt = stmt.preparedStatement();
    int hashIndexes[] = pstmt.getRoutingKeyIndexes();

    // Return if no hash key indexes are found, such as when the hash column values are literal
    // constants.
    if (hashIndexes == null || hashIndexes.length == 0) {
      return -1;
    }

    // Compute the hash key bytes, i.e. <h1><h2>...<h...>.
    try {
      ByteArrayOutputStream bs = new ByteArrayOutputStream();
      WritableByteChannel channel = Channels.newChannel(bs);
      ColumnDefinitions variables = pstmt.getVariables();
      for (int i = 0; i < hashIndexes.length; i++) {
        int index = hashIndexes[i];
        DataType type = variables.getType(index);
        ByteBuffer value = stmt.getBytesUnsafe(index);
        AppendValueToChannel(type, value, channel);
      }
      channel.close();

      return getKey(bs.toByteArray());
    } catch (IOException e) {
      // IOException should not happen at all given we are writing to the in-memory buffer only. So
      // if it does happen, we just want to log the error but fallback to the default set of hosts.
      logger.error("hash key encoding failed", e);
      return -1;
    }
  }

  private static void AppendValueToChannel(
      DataType type, ByteBuffer value, WritableByteChannel channel) throws java.io.IOException {
    DataType.Name typeName = type.getName();

    switch (typeName) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
      case ASCII:
      case TEXT:
      case JSONB:
      case VARCHAR:
      case BLOB:
      case INET:
      case UUID:
      case TIMEUUID:
      case DATE:
      case TIME:
        channel.write(value);
        break;
      case FLOAT:
        {
          float floatValue = value.getFloat(0);
          value.rewind();
          if (Float.isNaN(floatValue)) {
            // Normalize NaN byte representation.
            value = ByteBuffer.allocate(4);
            value.putInt(0xff << 23 | 0x1 << 22);
            value.flip();
          }
          channel.write(value);
          break;
        }
      case DOUBLE:
        {
          double doubleValue = value.getDouble(0);
          value.rewind();
          if (Double.isNaN(doubleValue)) {
            // Normalize NaN byte representation.
            value = ByteBuffer.allocate(8);
            value.putLong((long) 0x7ff << 52 | (long) 0x1 << 51);
            value.flip();
          }
          channel.write(value);
          break;
        }
      case TIMESTAMP:
        {
          // Multiply the timestamp's int64 value by 1000 to adjust the precision.
          ByteBuffer bb = ByteBuffer.allocate(8);
          bb.putLong(value.getLong() * 1000);
          bb.flip();
          value = bb;
          channel.write(value);
          break;
        }
      case LIST:
      case SET:
        {
          List<DataType> typeArgs = type.getTypeArguments();
          int length = value.getInt();
          for (int j = 0; j < length; j++) {
            // Appending each element.
            int size = value.getInt();
            ByteBuffer buf = value.slice();
            buf.limit(size);
            AppendValueToChannel(typeArgs.get(0), buf, channel);
            value.position(value.position() + size);
          }
          break;
        }
      case MAP:
        {
          List<DataType> typeArgs = type.getTypeArguments();
          int length = value.getInt();
          for (int j = 0; j < length; j++) {
            // Appending the key.
            int size = value.getInt();
            ByteBuffer buf = value.slice();
            buf.limit(size);
            AppendValueToChannel(typeArgs.get(0), buf, channel);
            value.position(value.position() + size);
            // Appending the value.
            size = value.getInt();
            buf = value.slice();
            buf.limit(size);
            AppendValueToChannel(typeArgs.get(1), buf, channel);
            value.position(value.position() + size);
          }
          break;
        }
      case UDT:
        {
          for (UserType.Field field : (UserType) type) {
            if (!value.hasRemaining()) {
              // UDT serialization may omit values of last few fields if they are null.
              break;
            }
            int size = value.getInt();
            ByteBuffer buf = value.slice();
            buf.limit(size);
            AppendValueToChannel(field.getType(), buf, channel);
            value.position(value.position() + size);
          }
          break;
        }
      case COUNTER:
      case CUSTOM:
      case DECIMAL:
      case TUPLE:
      case VARINT:
        throw new UnsupportedOperationException(
            "Datatype " + typeName.toString() + " not supported in a partition key column");
    }
  }

  /**
   * An iterator that returns hosts to executing a given statement, selecting only the hosts that
   * are up and local from the given hosts that host the statement's partition key, and then the
   * ones from the child policy.
   */
  private class UpHostIterator implements Iterator<Host> {

    private final String loggedKeyspace;
    private final Statement statement;
    private final List<Host> hosts;
    private final Iterator<Host> iterator;
    private Iterator<Host> childIterator;
    private Host nextHost;

    /**
     * Creates a new {@code UpHostIterator}.
     *
     * @param loggedKeyspace the logged keyspace of the statement
     * @param statement the statement
     * @param hosts the hosts that host the statement's partition key
     */
    public UpHostIterator(String loggedKeyspace, Statement statement, List<Host> hosts) {
      this.loggedKeyspace = loggedKeyspace;
      this.statement = statement;
      // When the CQL consistency level is set to YB consistent prefix (Cassandra ONE),
      // the reads would end up going only to the leader if the list of hosts are not shuffled.
      if (getConsistencyLevel() == ConsistencyLevel.YB_CONSISTENT_PREFIX) {
        Collections.shuffle(hosts);
      }
      this.hosts = hosts;
      this.iterator = hosts.iterator();
    }

    private ConsistencyLevel getConsistencyLevel() {
      return statement.getConsistencyLevel() != null
          ? statement.getConsistencyLevel()
          : configuration.getQueryOptions().getConsistencyLevel();
    }

    @Override
    public boolean hasNext() {

      while (iterator.hasNext()) {
        nextHost = iterator.next();
        // If the host is up, use it if it is local, or the statement requires strong consistency.
        // In the latter case, we want to use the first available host since the leader is in the
        // head of the host list.
        if (nextHost.isUp()
            && (childPolicy.distance(nextHost) == HostDistance.LOCAL
                || getConsistencyLevel().isYBStrong())) {
          return true;
        }
      }

      if (childIterator == null)
        childIterator = childPolicy.newQueryPlan(loggedKeyspace, statement);

      while (childIterator.hasNext()) {
        nextHost = childIterator.next();
        // Skip host if it is a local host that we have already returned earlier.
        if (!hosts.contains(nextHost)
            || !(childPolicy.distance(nextHost) == HostDistance.LOCAL
                || statement.getConsistencyLevel() == ConsistencyLevel.YB_STRONG)) return true;
      }

      return false;
    }

    @Override
    public Host next() {
      return nextHost;
    }
  }

  /**
   * Gets the query plan for a {@code BoundStatement}.
   *
   * @param loggedKeyspace the logged keyspace of the statement
   * @param statement the statement
   * @return the query plan, or null when no plan can be determined
   */
  private Iterator<Host> getQueryPlan(String loggedKeyspace, BoundStatement statement) {
    PreparedStatement pstmt = statement.preparedStatement();
    String query = pstmt.getQueryString();
    ColumnDefinitions variables = pstmt.getVariables();

    // Look up the hosts for the partition key. Skip statements that do not have bind variables.
    if (variables.size() == 0) return null;
    logger.debug("getQueryPlan: keyspace = " + loggedKeyspace + ", query = " + query);
    int key = getKey(statement);
    if (key < 0) return null;

    TableSplitMetadata tableSplitMetadata =
        clusterMetadata.getTableSplitMetadata(variables.getKeyspace(0), variables.getTable(0));
    if (tableSplitMetadata == null) {
      return null;
    }

    return new UpHostIterator(loggedKeyspace, statement, tableSplitMetadata.getHosts(key));
  }

  /**
   * Gets the query plan for a {@code BatchStatement}.
   *
   * @param loggedKeyspace the logged keyspace of the statement
   * @param batch the batch statement
   * @return the query plan, or null when no plan can be determined
   */
  private Iterator<Host> getQueryPlan(String loggedKeyspace, BatchStatement batch) {
    for (Statement statement : batch.getStatements()) {
      if (statement instanceof BoundStatement) {
        Iterator<Host> plan = getQueryPlan(loggedKeyspace, (BoundStatement) statement);
        if (plan != null) return plan;
      }
    }
    return null;
  }

  @Override
  public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
    Iterator<Host> plan = null;
    if (statement instanceof BoundStatement) {
      plan = getQueryPlan(loggedKeyspace, (BoundStatement) statement);
    } else if (statement instanceof BatchStatement) {
      plan = getQueryPlan(loggedKeyspace, (BatchStatement) statement);
    }
    return (plan != null) ? plan : childPolicy.newQueryPlan(loggedKeyspace, statement);
  }

  @Override
  public HostDistance distance(Host host) {
    return childPolicy.distance(host);
  }

  @Override
  public void onAdd(Host host) {
    childPolicy.onAdd(host);
  }

  @Override
  public void onUp(Host host) {
    childPolicy.onUp(host);
  }

  @Override
  public void onDown(Host host) {
    childPolicy.onDown(host);
  }

  @Override
  public void onRemove(Host host) {
    childPolicy.onRemove(host);
  }

  @Override
  public void close() {
    childPolicy.close();
  }
}
