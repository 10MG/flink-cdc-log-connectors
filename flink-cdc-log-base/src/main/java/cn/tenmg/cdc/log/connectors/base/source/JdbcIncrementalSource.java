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

package cn.tenmg.cdc.log.connectors.base.source;

import java.util.List;
import java.util.function.Supplier;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.kafka.connect.source.SourceRecord;

import cn.tenmg.cdc.log.connectors.base.config.JdbcSourceConfig;
import cn.tenmg.cdc.log.connectors.base.config.JdbcSourceConfigFactory;
import cn.tenmg.cdc.log.connectors.base.config.SourceConfig;
import cn.tenmg.cdc.log.connectors.base.dialect.JdbcDataSourceDialect;
import cn.tenmg.cdc.log.connectors.base.options.StartupMode;
import cn.tenmg.cdc.log.connectors.base.relational.JdbcSourceRecordEmitter;
import cn.tenmg.cdc.log.connectors.base.source.assigner.HybridSplitAssigner;
import cn.tenmg.cdc.log.connectors.base.source.assigner.SplitAssigner;
import cn.tenmg.cdc.log.connectors.base.source.assigner.StreamSplitAssigner;
import cn.tenmg.cdc.log.connectors.base.source.assigner.state.HybridPendingSplitsState;
import cn.tenmg.cdc.log.connectors.base.source.assigner.state.PendingSplitsState;
import cn.tenmg.cdc.log.connectors.base.source.assigner.state.PendingSplitsStateSerializer;
import cn.tenmg.cdc.log.connectors.base.source.assigner.state.StreamPendingSplitsState;
import cn.tenmg.cdc.log.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import cn.tenmg.cdc.log.connectors.base.source.meta.offset.OffsetFactory;
import cn.tenmg.cdc.log.connectors.base.source.meta.split.SourceSplitBase;
import cn.tenmg.cdc.log.connectors.base.source.meta.split.SourceSplitSerializer;
import cn.tenmg.cdc.log.connectors.base.source.metrics.SourceReaderMetrics;
import cn.tenmg.cdc.log.connectors.base.source.reader.JdbcIncrementalSourceReader;
import cn.tenmg.cdc.log.connectors.base.source.reader.JdbcSourceSplitReader;
import cn.tenmg.cdc.log.debezium.DebeziumDeserializationSchema;
import io.debezium.relational.TableId;

/**
 * The basic source of Incremental Snapshot framework for JDBC datasource, it is based on FLIP-27
 * and Watermark Signal Algorithm which supports parallel reading snapshot of table and then
 * continue to capture data change by streaming reading.
 */
@Experimental
public class JdbcIncrementalSource<T>
        implements Source<T, SourceSplitBase, PendingSplitsState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final JdbcSourceConfigFactory configFactory;
    private final JdbcDataSourceDialect dataSourceDialect;
    private final OffsetFactory offsetFactory;
    private final DebeziumDeserializationSchema<T> deserializationSchema;
    private final SourceSplitSerializer sourceSplitSerializer;

    public JdbcIncrementalSource(
            JdbcSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            OffsetFactory offsetFactory,
            JdbcDataSourceDialect dataSourceDialect) {
        this.configFactory = configFactory;
        this.deserializationSchema = deserializationSchema;
        this.offsetFactory = offsetFactory;
        this.dataSourceDialect = dataSourceDialect;
        this.sourceSplitSerializer =
                new SourceSplitSerializer() {
                    @Override
                    public OffsetFactory getOffsetFactory() {
                        return offsetFactory;
                    }
                };
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader createReader(SourceReaderContext readerContext) {
        // create source config for the given subtask (e.g. unique server id)
        JdbcSourceConfig sourceConfig = configFactory.create(readerContext.getIndexOfSubtask());
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        final SourceReaderMetrics sourceReaderMetrics =
                new SourceReaderMetrics(readerContext.metricGroup());
        sourceReaderMetrics.registerMetrics();
        Supplier<JdbcSourceSplitReader> splitReaderSupplier =
                () ->
                        new JdbcSourceSplitReader(
                                readerContext.getIndexOfSubtask(), dataSourceDialect);
        return new JdbcIncrementalSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                new JdbcSourceRecordEmitter<>(
                        deserializationSchema,
                        sourceReaderMetrics,
                        sourceConfig.isIncludeSchemaChanges(),
                        offsetFactory),
                readerContext.getConfiguration(),
                readerContext,
                sourceConfig,
                sourceSplitSerializer,
                dataSourceDialect);
    }

    @Override
    public SplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(
            SplitEnumeratorContext<SourceSplitBase> enumContext) {
        JdbcSourceConfig sourceConfig = configFactory.create(0);
        final SplitAssigner splitAssigner;
        if (sourceConfig.getStartupOptions().startupMode == StartupMode.INITIAL) {
            try {
                final List<TableId> remainingTables =
                        dataSourceDialect.discoverDataCollections(sourceConfig);
                boolean isTableIdCaseSensitive =
                        dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
                splitAssigner =
                        new HybridSplitAssigner(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                remainingTables,
                                isTableIdCaseSensitive,
                                dataSourceDialect,
                                offsetFactory);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover captured tables for enumerator", e);
            }
        } else {
            splitAssigner = new StreamSplitAssigner(sourceConfig, dataSourceDialect, offsetFactory);
        }

        return new IncrementalSourceEnumerator(enumContext, sourceConfig, splitAssigner);
    }

    @Override
    public SplitEnumerator<SourceSplitBase, PendingSplitsState> restoreEnumerator(
            SplitEnumeratorContext<SourceSplitBase> enumContext, PendingSplitsState checkpoint) {
        SourceConfig sourceConfig = configFactory.create(0);

        final SplitAssigner splitAssigner;
        if (checkpoint instanceof HybridPendingSplitsState) {
            splitAssigner =
                    new HybridSplitAssigner(
                            sourceConfig,
                            enumContext.currentParallelism(),
                            (HybridPendingSplitsState) checkpoint,
                            dataSourceDialect,
                            offsetFactory);
        } else if (checkpoint instanceof StreamPendingSplitsState) {
            splitAssigner =
                    new StreamSplitAssigner(
                            sourceConfig,
                            (StreamPendingSplitsState) checkpoint,
                            dataSourceDialect,
                            offsetFactory);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored PendingSplitsState: " + checkpoint);
        }
        return new IncrementalSourceEnumerator(enumContext, sourceConfig, splitAssigner);
    }

    @Override
    public SimpleVersionedSerializer<SourceSplitBase> getSplitSerializer() {
        return sourceSplitSerializer;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsState> getEnumeratorCheckpointSerializer() {
        SourceSplitSerializer sourceSplitSerializer = (SourceSplitSerializer) getSplitSerializer();
        return new PendingSplitsStateSerializer(sourceSplitSerializer);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
