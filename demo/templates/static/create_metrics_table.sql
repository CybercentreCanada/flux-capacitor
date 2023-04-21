
create table {{metrics_table}}
(
    batchId BIGINT,
    disk_used BIGINT,
    durationMs STRUCT < 
        addBatch: BIGINT,
        getBatch: BIGINT,
        latestOffset: BIGINT,
        queryPlanning: BIGINT,
        triggerExecution: BIGINT,
        walCommit: BIGINT 
        >,
    eventTime STRUCT < 
        avg: STRING,
        max: STRING,
        min: STRING,
        watermark: STRING 
        >,
    executor_memory STRING,
    id STRING,
    inputRowsPerSecond DOUBLE,
    name STRING,
    numInputRows BIGINT,
    processedRowsPerSecond DOUBLE,
    runId STRING,
    sink STRUCT < 
        description: STRING,
        numOutputRows: BIGINT 
        >,
    sources ARRAY < 
        STRUCT < 
            description: STRING,
            endOffset: BIGINT,
            inputRowsPerSecond: DOUBLE,
            latestOffset: BIGINT,
            numInputRows: BIGINT,
            processedRowsPerSecond: DOUBLE,
            startOffset: BIGINT 
            > 
        >,
    stateOperators ARRAY < 
        STRUCT < 
            allRemovalsTimeMs: BIGINT,
            allUpdatesTimeMs: BIGINT,
            commitTimeMs: BIGINT,
            customMetrics: STRUCT < 
                updateTagCacheTimer: BIGINT,
                getTagCount: BIGINT,
                putTagCount: BIGINT,
                sortTimer: BIGINT,
                fromStateTimer: BIGINT,
                toStateTimer: BIGINT,
                rocksdbBytesCopied: BIGINT,
                rocksdbCommitCheckpointLatency: BIGINT,
                rocksdbCommitCompactLatency: BIGINT,
                rocksdbCommitFileSyncLatencyMs: BIGINT,
                rocksdbCommitFlushLatency: BIGINT,
                rocksdbCommitPauseLatency: BIGINT,
                rocksdbCommitWriteBatchLatency: BIGINT,
                rocksdbFilesCopied: BIGINT,
                rocksdbFilesReused: BIGINT,
                rocksdbGetCount: BIGINT,
                rocksdbGetLatency: BIGINT,
                rocksdbPutCount: BIGINT,
                rocksdbPutLatency: BIGINT,
                rocksdbReadBlockCacheHitCount: BIGINT,
                rocksdbReadBlockCacheMissCount: BIGINT,
                rocksdbSstFileSize: BIGINT,
                rocksdbTotalBytesRead: BIGINT,
                rocksdbTotalBytesReadByCompaction: BIGINT,
                rocksdbTotalBytesReadThroughIterator: BIGINT,
                rocksdbTotalBytesWritten: BIGINT,
                rocksdbTotalBytesWrittenByCompaction: BIGINT,
                rocksdbTotalCompactionLatencyMs: BIGINT,
                rocksdbWriterStallLatencyMs: BIGINT,
                rocksdbZipFileBytesUncompressed: BIGINT 
            >,
            memoryUsedBytes: BIGINT,
            numRowsDroppedByWatermark: BIGINT,
            numRowsRemoved: BIGINT,
            numRowsTotal: BIGINT,
            numRowsUpdated: BIGINT,
            numShufflePartitions: BIGINT,
            numStateStoreInstances: BIGINT,
            operatorName: STRING 
            > 
        >,
    timestamp TIMESTAMP,
    worker_name STRING
)
using iceberg tblproperties (
    'commit.retry.num-retries' = '20',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '100',
    'read.split.target-size' = '33554432',
    'write.parquet.row-group-size-bytes' = '33554432',
    'write.spark.fanout.enabled' = 'true'
)
PARTITIONED BY (days(timestamp))