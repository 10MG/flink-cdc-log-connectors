/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.tenmg.cdc.log.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import cn.tenmg.cdc.log.debezium.JsonDebeziumDeserializationSchema;

import cn.tenmg.cdc.log.connectors.mysql.source.MySqlSource;
import cn.tenmg.cdc.log.connectors.mysql.testutils.UniqueDatabase;

import org.junit.Ignore;
import org.junit.Test;

/** Example Tests for {@link MySqlSource}. */
public class MySqlSourceExampleTest extends MySqlSourceTestBase {

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", "mysqluser", "mysqlpw");

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testConsumingAllEvents() throws Exception {
        inventoryDatabase.createAndInitialize();
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + ".products")
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .serverId("5401-5404")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // output the schema changes as well
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 4
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
                .setParallelism(4)
                .print()
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
