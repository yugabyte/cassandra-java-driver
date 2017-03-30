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

import org.testng.annotations.Test;

import static com.datastax.driver.core.ProtocolVersion.V4;

@CCMConfig(createCluster = false)
public class ChecksumTest extends CCMTestsSupport {

    @Test(groups = "short")
    public void should_connect_with_checksum() {
        Cluster cluster = super.createClusterBuilder()
                .addContactPointsWithPorts(ccm().addressOfNode(1))
                .allowBetaProtocolVersion()
                .build();
        cluster.connect();
    }

    @Test(groups = "short")
    public void should_connect_with_checksum_and_lz4_compression() {
        Cluster cluster = super.createClusterBuilder()
                .addContactPointsWithPorts(ccm().addressOfNode(1))
                .withCompression(ProtocolOptions.Compression.LZ4)
                .allowBetaProtocolVersion()
                .build();
        cluster.connect();
    }

    @Test(groups = "short")
    public void should_connect_without_checksum() {
        Cluster cluster = super.createClusterBuilder()
                .addContactPointsWithPorts(ccm().addressOfNode(1))
                .withProtocolVersion(V4)
                .build();
        cluster.connect();
    }

    @Test(groups = "short")
    public void should_connect_without_checksum_with_lz4_compression() {
        Cluster cluster = super.createClusterBuilder()
                .addContactPointsWithPorts(ccm().addressOfNode(1))
                .withCompression(ProtocolOptions.Compression.LZ4)
                .withProtocolVersion(V4)
                .build();
        cluster.connect();
    }

}
