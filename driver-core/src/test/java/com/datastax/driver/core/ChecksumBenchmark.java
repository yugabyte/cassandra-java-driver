/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.Message.Request.Type.QUERY;
import static com.datastax.driver.core.ProtocolVersion.V5;
import static org.openjdk.jmh.annotations.Threads.MAX;

@SuppressWarnings({"Duplicates", "UnusedReturnValue"})
public class ChecksumBenchmark {

    private static final EnumSet<Frame.Header.Flag> FLAGS_EMPTY = EnumSet.noneOf(Frame.Header.Flag.class);

    private static final EnumSet<Frame.Header.Flag> FLAGS_COMPRESSED = EnumSet.of(Frame.Header.Flag.COMPRESSED);

    private static final Requests.Query REQUEST = new Requests.Query("SELECT rpc_address FROM system.local");

    @SuppressWarnings("unchecked")
    private static final Message.Coder<Requests.Query> CODER = (Message.Coder<Requests.Query>) QUERY.coder;

    private static final int SIZE = CODER.encodedSize(REQUEST, V5);

    private static final String WRITE = "INSERT INTO ks.test (c1, c2, c3) VALUES (?, ?, 'lorem ipsum')";

    private static final String READ = "SELECT c1, c2, c3 FROM ks.test WHERE c1 = 0";

    @State(Scope.Benchmark)
    public static class ChecksumBenchmarkState {

        private static CCMBridge ccm;

        private Cluster clustera;
        private Cluster clusterb;
        private Cluster clusterc;
        private Cluster clusterd;

        private Session sessiona;
        private Session sessionc;
        private Session sessionb;
        private Session sessiond;

        @Setup(Level.Trial)
        public void setUp() {

            if (ccm == null) {
                System.setProperty("ccm.path", "/Users/alexandredutra/.jenv/shims:/Users/alexandredutra/.jenv/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:~/bin");
                ccm = CCMBridge.builder()
                        .withVersion(VersionNumber.parse("4.0"))
                        .build();
                ccm.start();
            }

            clustera = Cluster.builder().addContactPointsWithPorts(ccm.addressOfNode(1)).build();
            clusterb = Cluster.builder().addContactPointsWithPorts(ccm.addressOfNode(1)).withCompression(ProtocolOptions.Compression.LZ4).build();
            clusterc = Cluster.builder().addContactPointsWithPorts(ccm.addressOfNode(1)).allowBetaProtocolVersion().build();
            clusterd = Cluster.builder().addContactPointsWithPorts(ccm.addressOfNode(1)).withCompression(ProtocolOptions.Compression.LZ4).allowBetaProtocolVersion().build();

            sessiona = clustera.connect();
            sessionb = clusterb.connect();
            sessionc = clusterc.connect();
            sessiond = clusterd.connect();

            sessiona.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
            sessiona.execute("CREATE TABLE IF NOT EXISTS ks.test (c1 int, c2 int, c3 varchar, PRIMARY KEY (c1, c2, c3))");
            sessiona.execute("TRUNCATE ks.test");

            for (int i = 0; i < 100; i++) {
                sessiona.execute(WRITE, 0, 1);
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            clustera.close();
            clusterb.close();
            clusterc.close();
            clusterd.close();
        }

    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(1)
    @Fork(1)
    public Frame benchmark_1aa_micro_1_core_base() throws IOException {
        ByteBuf body = ByteBufAllocator.DEFAULT.buffer(SIZE);
        CODER.encode(REQUEST, body, V5);
        Frame frame = Frame.create(V5, QUERY.opcode, 1, FLAGS_EMPTY, body);
        frame.body.release();
        return frame;
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(1)
    @Fork(1)
    public Frame[] benchmark_1ab_micro_1_core_LZ4() throws IOException {
        ByteBuf body = ByteBufAllocator.DEFAULT.buffer(SIZE);
        CODER.encode(REQUEST, body, V5);
        Frame frame1 = Frame.create(V5, QUERY.opcode, 1, FLAGS_COMPRESSED, body);
        Frame frame2 = LZ4Compressor.INSTANCE.compress(frame1);
        frame1.body.release();
        frame2.body.release();
        return new Frame[]{frame1, frame2};
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(1)
    @Fork(1)
    public Frame[] benchmark_1ac_micro_1_core_checksum() throws IOException {
        ByteBuf body = ByteBufAllocator.DEFAULT.buffer(SIZE);
        CODER.encode(REQUEST, body, V5);
        Frame frame1 = Frame.create(V5, QUERY.opcode, 1, FLAGS_EMPTY, body);
        Frame frame2 = ChecksumCompressor.INSTANCE.compress(frame1);
        frame1.body.release();
        frame2.body.release();
        return new Frame[]{frame1, frame2};
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(1)
    @Fork(1)
    public Frame[] benchmark_1ad_micro_1_core_LZ4_checksum() throws IOException {
        ByteBuf body = ByteBufAllocator.DEFAULT.buffer(SIZE);
        CODER.encode(REQUEST, body, V5);
        Frame frame1 = Frame.create(V5, QUERY.opcode, 1, FLAGS_COMPRESSED, body);
        Frame frame2 = LZ4ChecksumCompressor.INSTANCE.compress(frame1);
        frame1.body.release();
        frame2.body.release();
        return new Frame[]{frame1, frame2};
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(MAX)
    @Fork(1)
    public Frame benchmark_1ba_micro_max_cores_base() throws IOException {
        ByteBuf body = ByteBufAllocator.DEFAULT.buffer(SIZE);
        CODER.encode(REQUEST, body, V5);
        Frame frame = Frame.create(V5, QUERY.opcode, 1, FLAGS_EMPTY, body);
        frame.body.release();
        return frame;
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(MAX)
    @Fork(1)
    public Frame[] benchmark_1bb_micro_max_cores_LZ4() throws IOException {
        ByteBuf body = ByteBufAllocator.DEFAULT.buffer(SIZE);
        CODER.encode(REQUEST, body, V5);
        Frame frame1 = Frame.create(V5, QUERY.opcode, 1, FLAGS_COMPRESSED, body);
        Frame frame2 = LZ4Compressor.INSTANCE.compress(frame1);
        frame1.body.release();
        frame2.body.release();
        return new Frame[]{frame1, frame2};
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(MAX)
    @Fork(1)
    public Frame[] benchmark_1bc_micro_max_cores_checksum() throws IOException {
        ByteBuf body = ByteBufAllocator.DEFAULT.buffer(SIZE);
        CODER.encode(REQUEST, body, V5);
        Frame frame1 = Frame.create(V5, QUERY.opcode, 1, FLAGS_EMPTY, body);
        Frame frame2 = ChecksumCompressor.INSTANCE.compress(frame1);
        frame1.body.release();
        frame2.body.release();
        return new Frame[]{frame1, frame2};
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(MAX)
    @Fork(1)
    public Frame[] benchmark_1bd_micro_max_cores_LZ4_checksum() throws IOException {
        ByteBuf body = ByteBufAllocator.DEFAULT.buffer(SIZE);
        CODER.encode(REQUEST, body, V5);
        Frame frame1 = Frame.create(V5, QUERY.opcode, 1, FLAGS_COMPRESSED, body);
        Frame frame2 = LZ4ChecksumCompressor.INSTANCE.compress(frame1);
        frame1.body.release();
        frame2.body.release();
        return new Frame[]{frame1, frame2};
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(1)
    @Fork(1)
    public Row benchmark_2aa_write_1_core_base(ChecksumBenchmarkState state) throws IOException {
        return state.sessiona.execute(WRITE, 1, 2).one();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(1)
    @Fork(1)
    public Row benchmark_2ab_write_1_core_LZ4(ChecksumBenchmarkState state) throws IOException {
        return state.sessionb.execute(WRITE, 1, 2).one();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(1)
    @Fork(1)
    public Row benchmark_2ac_write_1_core_checksum(ChecksumBenchmarkState state) throws IOException {
        return state.sessionc.execute(WRITE, 1, 2).one();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(1)
    @Fork(1)
    public Row benchmark_2ad_write_1_core_LZ4_checksum(ChecksumBenchmarkState state) throws IOException {
        return state.sessiond.execute(WRITE, 1, 2).one();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(MAX)
    @Fork(1)
    public Row benchmark_2ba_write_max_cores_base(ChecksumBenchmarkState state) throws IOException {
        return state.sessiona.execute(WRITE, 1, 2).one();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(MAX)
    @Fork(1)
    public Row benchmark_2bb_write_max_cores_LZ4(ChecksumBenchmarkState state) throws IOException {
        return state.sessionb.execute(WRITE, 1, 2).one();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(MAX)
    @Fork(1)
    public Row benchmark_2bc_write_max_cores_checksum(ChecksumBenchmarkState state) throws IOException {
        return state.sessionc.execute(WRITE, 1, 2).one();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(MAX)
    @Fork(1)
    public Row benchmark_2bd_write_max_cores_LZ4_checksum(ChecksumBenchmarkState state) throws IOException {
        return state.sessiond.execute(WRITE, 1, 2).one();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(1)
    @Fork(1)
    public List<Row> benchmark_3aa_read_1_core_base(ChecksumBenchmarkState state) throws IOException {
        return state.sessiona.execute(READ).all();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(1)
    @Fork(1)
    public List<Row> benchmark_3ab_read_1_core_LZ4(ChecksumBenchmarkState state) throws IOException {
        return state.sessionb.execute(READ).all();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(1)
    @Fork(1)
    public List<Row> benchmark_3ac_read_1_core_checksum(ChecksumBenchmarkState state) throws IOException {
        return state.sessionc.execute(READ).all();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(1)
    @Fork(1)
    public List<Row> benchmark_3ad_read_1_core_LZ4_checksum(ChecksumBenchmarkState state) throws IOException {
        return state.sessiond.execute(READ).all();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(MAX)
    @Fork(1)
    public List<Row> benchmark_3ba_read_max_cores_base(ChecksumBenchmarkState state) throws IOException {
        return state.sessiona.execute(READ).all();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(MAX)
    @Fork(1)
    public List<Row> benchmark_3bb_read_max_cores_LZ4(ChecksumBenchmarkState state) throws IOException {
        return state.sessionb.execute(READ).all();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(MAX)
    @Fork(1)
    public List<Row> benchmark_3bc_read_max_cores_checksum(ChecksumBenchmarkState state) throws IOException {
        return state.sessionc.execute(READ).all();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Threads(MAX)
    @Fork(1)
    public List<Row> benchmark_3bd_read_max_cores_LZ4_checksum(ChecksumBenchmarkState state) throws IOException {
        return state.sessiond.execute(READ).all();
    }

}
