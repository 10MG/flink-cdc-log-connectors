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

package cn.tenmg.cdc.log.connectors.oceanbase.source;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.mysql.jdbc.ResultSetMetaData;
import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ClientConf;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.clogproxy.client.util.ClientIdGenerator;
import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import cn.tenmg.cdc.log.debezium.DebeziumDeserializationSchema;
import io.debezium.relational.TableSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * The source implementation for OceanBase that read snapshot events first and then read the change
 * event.
 *
 * @param <T> The type created by the deserializer.
 */
public class OceanBaseRichSourceFunction<T> extends RichSourceFunction<T>
        implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 2844054619864617340L;

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseRichSourceFunction.class);

    private final boolean snapshot;
    private final String username;
    private final String password;
    private final String tenantName;
    private final String databaseName;
    private final String tableName;
    private final String hostname;
    private final Integer port;
    private final Duration connectTimeout;
    private final String rsList;
    private final String logProxyHost;
    private final int logProxyPort;
    private final long startTimestamp;
    private final ZoneOffset zoneOffset;
    private final DebeziumDeserializationSchema<T> deserializer;

    private final AtomicBoolean snapshotCompleted = new AtomicBoolean(false);
    private final List<LogMessage> logMessageBuffer = new LinkedList<>();

    private transient Map<String, TableSchema> tableSchemaMap;
    private transient volatile long resolvedTimestamp;
    private transient volatile OceanBaseConnection snapshotConnection;
    private transient LogProxyClient logProxyClient;
    private transient ListState<Long> offsetState;
    private transient OutputCollector<T> outputCollector;

    public OceanBaseRichSourceFunction(
            boolean snapshot,
            long startTimestamp,
            String username,
            String password,
            String tenantName,
            String databaseName,
            String tableName,
            String hostname,
            Integer port,
            Duration connectTimeout,
            String rsList,
            String logProxyHost,
            int logProxyPort,
            ZoneId serverTimeZone,
            DebeziumDeserializationSchema<T> deserializer) {
        this.snapshot = snapshot;
        this.username = username;
        this.password = password;
        this.tenantName = tenantName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.hostname = hostname;
        this.port = port;
        this.connectTimeout = connectTimeout;
        this.rsList = rsList;
        this.logProxyHost = logProxyHost;
        this.logProxyPort = logProxyPort;
        this.startTimestamp = startTimestamp;
        this.zoneOffset = serverTimeZone.getRules().getOffset(Instant.now());
        this.deserializer = deserializer;
    }

    @Override
    public void open(final Configuration config) throws Exception {
        super.open(config);
        this.outputCollector = new OutputCollector<>();
        this.tableSchemaMap = new ConcurrentHashMap<>();
        this.resolvedTimestamp = -1;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        outputCollector.context = ctx;

        LOG.info("Start readChangeEvents process");
        readChangeEvents();

        if (shouldReadSnapshot()) {
            synchronized (ctx.getCheckpointLock()) {
                try {
                    if (snapshotConnection == null) {
                        snapshotConnection =
                                new OceanBaseConnection(
                                        hostname,
                                        port,
                                        username,
                                        password,
                                        connectTimeout,
                                        getClass().getClassLoader());
                    }
                    readSnapshot();
                } finally {
                    if (snapshotConnection != null) {
                        snapshotConnection.close();
                    }
                }
                LOG.info("Snapshot reading finished");
            }
        } else {
            LOG.info("Skip snapshot read");
        }

        logProxyClient.join();
    }

    protected void readSnapshot() {
        final Map<String, String> tableMap = new HashMap<>();
        try {
            String sql =
                    String.format(
                            "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                                    + "WHERE TABLE_TYPE='BASE TABLE' and TABLE_SCHEMA REGEXP '%s' and TABLE_NAME REGEXP '%s'",
                            databaseName, tableName);
            snapshotConnection.query(
                    sql,
                    rs -> {
                        while (rs.next()) {
                            tableMap.put(rs.getString(1), rs.getString(2));
                        }
                    });
        } catch (SQLException e) {
            LOG.error("Query database and table name failed", e);
            throw new FlinkRuntimeException(e);
        }
        tableMap.forEach(this::readSnapshotFromTable);
        snapshotCompleted.set(true);
    }

    private void readSnapshotFromTable(String databaseName, String tableName) {
        // TODO make topic name configurable
        String topicName = getDefaultTopicName(tenantName, databaseName, tableName);
        Map<String, String> partition = getSourcePartition(tenantName, databaseName, tableName);
        // the offset here is useless
        Map<String, Object> offset = getSourceOffset(resolvedTimestamp);

        String fullName = String.format("`%s`.`%s`", databaseName, tableName);
        String selectSql = "SELECT * FROM " + fullName;
        try {
            snapshotConnection.query(
                    selectSql,
                    rs -> {
                        ResultSetMetaData metaData = (ResultSetMetaData) rs.getMetaData();
                        String[] columnNames = new String[metaData.getColumnCount()];
                        int[] jdbcTypes = new int[metaData.getColumnCount()];
                        for (int i = 0; i < metaData.getColumnCount(); i++) {
                            columnNames[i] = metaData.getColumnName(i + 1);
                            jdbcTypes[i] =
                                    OceanBaseJdbcConverter.getType(
                                            metaData.getColumnType(i + 1),
                                            metaData.getColumnTypeName(i + 1));
                        }

                        TableSchema tableSchema = tableSchemaMap.get(topicName);
                        if (tableSchema == null) {
                            tableSchema =
                                    OceanBaseTableSchema.getTableSchema(
                                            topicName,
                                            databaseName,
                                            tableName,
                                            columnNames,
                                            jdbcTypes,
                                            zoneOffset);
                            tableSchemaMap.put(topicName, tableSchema);
                        }

                        Struct source =
                                OceanBaseSchemaUtils.sourceStruct(
                                        tenantName, databaseName, tableName, null, null);

                        while (rs.next()) {
                            Struct value = new Struct(tableSchema.valueSchema());
                            for (int i = 0; i < metaData.getColumnCount(); i++) {
                                value.put(
                                        columnNames[i],
                                        OceanBaseJdbcConverter.getField(
                                                jdbcTypes[i], rs.getObject(i + 1)));
                            }
                            Struct struct =
                                    tableSchema.getEnvelopeSchema().create(value, source, null);
                            try {
                                deserializer.deserialize(
                                        new SourceRecord(
                                                partition,
                                                offset,
                                                topicName,
                                                null,
                                                null,
                                                null,
                                                struct.schema(),
                                                struct),
                                        outputCollector);
                            } catch (Exception e) {
                                LOG.error("Deserialize snapshot record failed ", e);
                                throw new FlinkRuntimeException(e);
                            }
                        }
                    });
        } catch (SQLException e) {
            LOG.error("Read snapshot from table " + fullName + " failed", e);
            throw new FlinkRuntimeException(e);
        }
    }

    protected void readChangeEvents() throws InterruptedException {
        String tableWhiteList = String.format("%s.%s.%s", tenantName, databaseName, tableName);
        ObReaderConfig obReaderConfig = new ObReaderConfig();
        obReaderConfig.setRsList(rsList);
        obReaderConfig.setUsername(username);
        obReaderConfig.setPassword(password);
        obReaderConfig.setTableWhiteList(tableWhiteList);

        if (resolvedTimestamp > 0) {
            obReaderConfig.setStartTimestamp(resolvedTimestamp);
            LOG.info("Read change events from resolvedTimestamp: {}", resolvedTimestamp);
        } else {
            obReaderConfig.setStartTimestamp(startTimestamp);
            LOG.info("Read change events from startTimestamp: {}", startTimestamp);
        }

        final CountDownLatch latch = new CountDownLatch(1);

        // avoid client id duplication when starting multiple connectors in one etl
        ClientConf.USER_DEFINED_CLIENTID = ClientIdGenerator.generate() + tableWhiteList;
        logProxyClient = new LogProxyClient(logProxyHost, logProxyPort, obReaderConfig);

        logProxyClient.addListener(
                new RecordListener() {

                    boolean started = false;

                    @Override
                    public void notify(LogMessage message) {
                        switch (message.getOpt()) {
                            case HEARTBEAT:
                            case BEGIN:
                                if (!started) {
                                    started = true;
                                    latch.countDown();
                                }
                                break;
                            case INSERT:
                            case UPDATE:
                            case DELETE:
                                if (!started) {
                                    break;
                                }
                                logMessageBuffer.add(message);
                                break;
                            case COMMIT:
                                // flush buffer after snapshot completed
                                if (!shouldReadSnapshot() || snapshotCompleted.get()) {
                                    logMessageBuffer.forEach(
                                            msg -> {
                                                try {
                                                    deserializer.deserialize(
                                                            getRecordFromLogMessage(msg),
                                                            outputCollector);
                                                } catch (Exception e) {
                                                    throw new FlinkRuntimeException(e);
                                                }
                                            });
                                    logMessageBuffer.clear();
                                    resolvedTimestamp = Long.parseLong(message.getTimestamp());
                                }
                                break;
                            case DDL:
                                // TODO record ddl and remove expired table schema
                                LOG.trace(
                                        "Ddl: {}",
                                        message.getFieldList().get(0).getValue().toString());
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported type: " + message.getOpt());
                        }
                    }

                    @Override
                    public void onException(LogProxyClientException e) {
                        LOG.error("LogProxyClient exception", e);
                        logProxyClient.stop();
                    }
                });

        logProxyClient.start();
        LOG.info("LogProxyClient started");
        latch.await();
        LOG.info("LogProxyClient packet processing started");
    }

    private SourceRecord getRecordFromLogMessage(LogMessage message) throws Exception {
        String databaseName = message.getDbName().replace(tenantName + ".", "");
        // TODO make topic name configurable
        String topicName = getDefaultTopicName(tenantName, databaseName, message.getTableName());

        if (tableSchemaMap.get(topicName) == null) {
            String[] columnNames = new String[message.getFieldCount()];
            int[] jdbcTypes = new int[message.getFieldCount()];
            int i = 0;
            for (DataMessage.Record.Field field : message.getFieldList()) {
                if (message.getOpt() == DataMessage.Record.Type.UPDATE && field.isPrev()) {
                    continue;
                }
                columnNames[i] = field.getFieldname();
                jdbcTypes[i] = OceanBaseJdbcConverter.getType(field.getType());
                i++;
            }
            TableSchema tableSchema =
                    OceanBaseTableSchema.getTableSchema(
                            topicName, databaseName, tableName, columnNames, jdbcTypes, zoneOffset);
            tableSchemaMap.put(topicName, tableSchema);
        }

        Struct source =
                OceanBaseSchemaUtils.sourceStruct(
                        tenantName,
                        databaseName,
                        message.getTableName(),
                        message.getTimestamp(),
                        message.getOB10UniqueId());
        Struct struct;
        switch (message.getOpt()) {
            case INSERT:
                Struct after = getLogValueStruct(topicName, message.getFieldList());
                struct =
                        tableSchemaMap
                                .get(topicName)
                                .getEnvelopeSchema()
                                .create(after, source, null);
                break;
            case UPDATE:
                List<DataMessage.Record.Field> beforeFields = new ArrayList<>();
                List<DataMessage.Record.Field> afterFields = new ArrayList<>();
                for (DataMessage.Record.Field field : message.getFieldList()) {
                    if (field.isPrev()) {
                        beforeFields.add(field);
                    } else {
                        afterFields.add(field);
                    }
                }
                after = getLogValueStruct(topicName, afterFields);
                Struct before = getLogValueStruct(topicName, beforeFields);
                struct =
                        tableSchemaMap
                                .get(topicName)
                                .getEnvelopeSchema()
                                .update(before, after, source, null);
                break;
            case DELETE:
                before = getLogValueStruct(topicName, message.getFieldList());
                struct =
                        tableSchemaMap
                                .get(topicName)
                                .getEnvelopeSchema()
                                .delete(before, source, null);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported dml type: " + message.getOpt());
        }
        return new SourceRecord(
                getSourcePartition(tenantName, databaseName, message.getTableName()),
                getSourceOffset(Long.parseLong(message.getTimestamp())),
                topicName,
                null,
                null,
                null,
                struct.schema(),
                struct);
    }

    private boolean shouldReadSnapshot() {
        return resolvedTimestamp == -1 && snapshot;
    }

    private String getDefaultTopicName(String tenantName, String databaseName, String tableName) {
        return String.format("%s.%s.%s", tenantName, databaseName, tableName);
    }

    private Map<String, String> getSourcePartition(
            String tenantName, String databaseName, String tableName) {
        Map<String, String> sourcePartition = new HashMap<>();
        sourcePartition.put("tenant", tenantName);
        sourcePartition.put("database", databaseName);
        sourcePartition.put("table", tableName);
        return sourcePartition;
    }

    private Map<String, Object> getSourceOffset(long timestamp) {
        Map<String, Object> sourceOffset = new HashMap<>();
        sourceOffset.put("timestamp", timestamp);
        return sourceOffset;
    }

    private Struct getLogValueStruct(String topicName, List<DataMessage.Record.Field> fieldList) {
        TableSchema tableSchema = tableSchemaMap.get(topicName);
        Struct value = new Struct(tableSchema.valueSchema());
        Object fieldValue;
        for (DataMessage.Record.Field field : fieldList) {
            try {
                Schema fieldSchema = tableSchema.valueSchema().field(field.getFieldname()).schema();
                fieldValue =
                        OceanBaseJdbcConverter.getField(
                                fieldSchema.type(), field.getType(), field.getValue());
                value.put(field.getFieldname(), fieldValue);
            } catch (NumberFormatException e) {
                tableSchema =
                        OceanBaseTableSchema.upcastingTableSchema(
                                topicName,
                                tableSchema,
                                fieldList.stream()
                                        .collect(
                                                Collectors.toMap(
                                                        DataMessage.Record.Field::getFieldname,
                                                        f -> f.getValue().toString())));
                tableSchemaMap.put(topicName, tableSchema);
                return getLogValueStruct(topicName, fieldList);
            }
        }
        return value;
    }

    @Override
    public void notifyCheckpointComplete(long l) {
        // do nothing
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.deserializer.getProducedType();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.info(
                "snapshotState checkpoint: {} at resolvedTimestamp: {}",
                context.getCheckpointId(),
                resolvedTimestamp);
        offsetState.clear();
        offsetState.add(resolvedTimestamp);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("initialize checkpoint");
        offsetState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "resolvedTimestampState", LongSerializer.INSTANCE));
        if (context.isRestored()) {
            for (final Long offset : offsetState.get()) {
                resolvedTimestamp = offset;
                LOG.info("Restore State from resolvedTimestamp: {}", resolvedTimestamp);
                return;
            }
        }
    }

    @Override
    public void cancel() {
        try {
            if (snapshotConnection != null) {
                snapshotConnection.close();
            }
        } catch (SQLException e) {
            LOG.error("Failed to close snapshotConnection", e);
        }
        if (logProxyClient != null) {
            logProxyClient.stop();
        }
    }

    private static class OutputCollector<T> implements Collector<T> {

        private SourceContext<T> context;

        @Override
        public void collect(T record) {
            context.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
