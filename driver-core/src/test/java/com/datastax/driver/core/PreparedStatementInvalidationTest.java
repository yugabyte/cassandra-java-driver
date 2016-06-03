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


import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;

@CassandraVersion("3.12")
public class PreparedStatementInvalidationTest extends CCMTestsSupport {

    @Override
    public Cluster.Builder createClusterBuilder() {
        return super.createClusterBuilderNoDebouncing().allowBetaProtocolVersion();
    }

    @BeforeMethod
    public void createTable() throws Exception {
        execute("CREATE TABLE PreparedStatementInvalidationTest (a int PRIMARY KEY, b int, c int);");
    }

    @AfterMethod
    public void dropTable() throws Exception {
        execute("DROP TABLE IF EXISTS PreparedStatementInvalidationTest");
    }

    @Test(groups = "short")
    public void should_update_statement_id_when_metadata_changed() {
        // given
        PreparedStatement ps = session().prepare("SELECT * FROM PreparedStatementInvalidationTest WHERE a = ?");
        MD5Digest idBefore = ps.getPreparedId().resultSetMetadata.id;
        // when
        session().execute("ALTER TABLE PreparedStatementInvalidationTest ADD d int");
        BoundStatement bs = ps.bind(1);
        ResultSet rows = session().execute(bs);
        // then
        MD5Digest idAfter = ps.getPreparedId().resultSetMetadata.id;
        assertThat(idBefore).isNotEqualTo(idAfter);
        assertThat(ps.getPreparedId().resultSetMetadata.variables)
                .hasSize(4)
                .containsVariable("d", DataType.cint());
        assertThat(bs.preparedStatement().getPreparedId().resultSetMetadata.variables)
                .hasSize(4)
                .containsVariable("d", DataType.cint());
        assertThat(rows.getColumnDefinitions())
                .hasSize(4)
                .containsVariable("d", DataType.cint());
    }

    @Test(groups = "short")
    public void should_update_statement_id_when_metadata_changed_across_sessions() {
        Session session1 = cluster().connect();
        useKeyspace(session1, keyspace);
        Session session2 = cluster().connect();
        useKeyspace(session2, keyspace);

        PreparedStatement ps1 = session1.prepare("SELECT * FROM PreparedStatementInvalidationTest WHERE a = ?");
        PreparedStatement ps2 = session2.prepare("SELECT * FROM PreparedStatementInvalidationTest WHERE a = ?");

        MD5Digest id1a = ps1.getPreparedId().resultSetMetadata.id;
        MD5Digest id2a = ps2.getPreparedId().resultSetMetadata.id;

        ResultSet rows1 = session1.execute(ps1.bind(1));
        ResultSet rows2 = session2.execute(ps2.bind(1));

        assertThat(rows1.getColumnDefinitions())
                .hasSize(3)
                .containsVariable("a", DataType.cint())
                .containsVariable("b", DataType.cint())
                .containsVariable("c", DataType.cint());
        assertThat(rows2.getColumnDefinitions())
                .hasSize(3)
                .containsVariable("a", DataType.cint())
                .containsVariable("b", DataType.cint())
                .containsVariable("c", DataType.cint());

        session1.execute("ALTER TABLE PreparedStatementInvalidationTest ADD d int");

        rows1 = session1.execute(ps1.bind(1));
        rows2 = session2.execute(ps2.bind(1));

        MD5Digest id1b = ps1.getPreparedId().resultSetMetadata.id;
        MD5Digest id2b = ps2.getPreparedId().resultSetMetadata.id;

        assertThat(id1a).isNotEqualTo(id1b);
        assertThat(id2a).isNotEqualTo(id2b);

        assertThat(ps1.getPreparedId().resultSetMetadata.variables)
                .hasSize(4)
                .containsVariable("d", DataType.cint());
        assertThat(ps2.getPreparedId().resultSetMetadata.variables)
                .hasSize(4)
                .containsVariable("d", DataType.cint());
        assertThat(rows1.getColumnDefinitions())
                .hasSize(4)
                .containsVariable("d", DataType.cint());
        assertThat(rows2.getColumnDefinitions())
                .hasSize(4)
                .containsVariable("d", DataType.cint());
    }

    @Test(groups = "short", expectedExceptions = NoHostAvailableException.class)
    public void should_not_reprepare_invalid_statements() {
        // given
        session().execute("ALTER TABLE PreparedStatementInvalidationTest ADD d int");
        PreparedStatement ps = session().prepare("SELECT a, b, c, d FROM PreparedStatementInvalidationTest WHERE a = ?");
        session().execute("ALTER TABLE PreparedStatementInvalidationTest DROP d");
        // when
        session().execute(ps.bind());
    }
}
