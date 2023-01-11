
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import DataFrame
import time
import argparse
import sys
import json
import os

from textwrap import dedent

spill = '/tmp/localtemp/sparkspill/'


def flux_capacitor(input_df, spec, bloom_capacity, group_col):
    """Python function to invoke the scala code"""
    spark = SparkSession.builder.getOrCreate()
    flux_stateful_function = spark._sc._jvm.cccs.fluxcapacitor.FluxCapacitor.invoke
    jdf = flux_stateful_function(
            input_df._jdf, 
            group_col, 
            bloom_capacity, 
            spec)
    return DataFrame(jdf, spark)


executor_metrics_schema = StructType([
    StructField('worker_name', StringType()),
    StructField('total', LongType()),
    StructField("used", LongType()),
    StructField("free", LongType()),
    StructField("executor_memory", StringType()),
])


def executor_metrics(dir_path):
    import shutil
    import socket
    import os
    worker_name = socket.gethostname()
    total, used, free = shutil.disk_usage(dir_path)
    stream = os.popen("ps auxww|grep -v grep | grep java | grep driver-url")
    output = stream.read()
    stream.close()
    executor_memory = int(output.split()[5])
    return (worker_name, total, used, free, executor_memory)


def calculate_metrics(spark, query):
    metrics = {}
    if query.lastProgress:
        stateOperator = query.lastProgress['stateOperators'][0]
        if stateOperator and stateOperator['numRowsTotal']:
            disk = spark.sql(f"select executor_metrics('{spill}')").collect()[0][0]
            worker_name = disk['worker_name']
            total = disk['total']
            disk_used = disk['used']
            free = disk['free']
            executor_memory = disk['executor_memory']
            metrics = query.lastProgress.copy()
            metrics.update({
                "worker_name": worker_name,
                "disk_used": disk_used,
                "executor_memory": executor_memory,
            })

    return metrics


def write_metrics(name, metric):
    os.makedirs("telemetry", exist_ok=True)
    with open(f"telemetry/{name}.log.json", "a") as f:
        f.write(json.dumps(metric) + '\n')

def do_metrics(spark, name, query):
    metric = calculate_metrics(spark, query)
    write_metrics(name, metric)

def start_query(conf):
    
    output_table = f'experiment.jcc.{conf.name}'
    checkpoint = f'{conf.catalog}/jcc/{conf.name}/checkpoint/'

    builder = (
        SparkSession.builder
            .appName(conf.name)
            .master("spark://ver-1-spark-master-svc.spark:7077")
            .config("spark.sql.shuffle.partitions", conf.partitions)
            .config("spark.driver.cores", 1)
            .config("spark.driver.memory", "2G")
            .config("spark.executor.memory", conf.xmx)
            .config("spark.cores.max", 16)
            .config("spark.executor.cores", 16)
            .config("spark.sql.catalog.experiment", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.experiment.type", "hadoop")
            .config("spark.sql.catalog.experiment.warehouse", conf.catalog)
            .config('spark.jars', './target/flux-capacitor-1.jar')
            .config("spark.sql.streaming.maxBatchesToRetainInMemory", 1)
    )
    
    if conf.store == 'rocksdb':
        builder = (builder
            .config("spark.sql.streaming.stateStore.providerClass", 
                    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
                   )
    if conf.store == 'fluxstore':
        builder = (builder
            .config("spark.sql.streaming.stateStore.providerClass", 
                    "org.apache.spark.sql.execution.streaming.state.FluxStateStoreProvider")
                   )
    
    spark = builder.getOrCreate()
    
    spark.udf.register("executor_metrics", executor_metrics, executor_metrics_schema)

    hosts = spark.range(conf.numhosts).withColumn('a_uuid', F.expr('uuid()')).withColumnRenamed('id', 'host_id')
    hosts.persist()
    hosts.createOrReplaceTempView("hosts")
    hosts.show()
    print(hosts.count())
    
    df = (
        spark.readStream
        .format("rate-micro-batch")
        .option("rowsPerBatch", conf.rate * conf.trigger)
        .load()
        .withWatermark("timestamp", "0 seconds")
        .withColumn("name", F.lit(conf.name))
    )

    df.createOrReplaceTempView("events")
    
    if conf.distribution == "roundrobin":
        join_condition = f"value % {conf.numhosts} = host_id"
    else:
        # generations 
        # at a rate of r5000 and a trigger of 5min t300
        # we have 1.5M events per micro-batch
        # 1.5M / 50k hosts = 30
        # instead of using modulo like in round robin we will divide
        # this simulates getting telemery from 30 machines, per micro-batch
        join_condition = f"((cast(value / 300000 as int) * 1000) + (value % 1000)) % {conf.numhosts} = id"
    
    print(join_condition)
    
    sql = f"""
    select *, sigma as sigma_input from (
         select
             a_uuid,
             a_uuid || '_' || value || '_' || 'childkey' AS key,
             a_uuid || '_' || value || '_' || 'parentkey' AS parent_key,
             map(
                "rule1", map(
                            "k1",
                            rand() > 0.9,
                            "k2",
                            rand() > 0.9,
                            "k3",
                            rand() > 0.9,
                            "k4",
                            rand() > 0.9,
                            "k5",
                            rand() > 0.9,
                            "k6",
                            rand() > 0.9,
                            "k7",
                            rand() > 0.9,
                            "k8",
                            rand() > 0.9,
                            "k9",
                            rand() > 0.9,
                            "k10",
                            rand() > 0.9,
                            "k11",
                            rand() > 0.9,
                            "k12",
                            rand() > 0.9,
                            "k13",
                            rand() > 0.9,
                            "k14",
                            rand() > 0.9,
                            "k15",
                            rand() > 0.9,
                            "k16",
                            rand() > 0.9,
                            "k17",
                            rand() > 0.9,
                            "k18",
                            rand() > 0.9,
                            "k19",
                            rand() > 0.9,
                            "k20",
                            rand() > 0.9,
                            "k21",
                            rand() > 0.9,
                            "k22",
                            rand() > 0.9,
                            "k23",
                            rand() > 0.9,
                            "k24",
                            rand() > 0.9,
                            "k25",
                            rand() > 0.9,
                            "m1",
                            rand() > 0.9,
                            "m2",
                            rand() > 0.9,
                            "m3",
                            rand() > 0.9,
                            "m4",
                            rand() > 0.9,
                            "m5",
                            rand() > 0.9,
                            "m6",
                            rand() > 0.9,
                            "m7",
                            rand() > 0.9,
                            "m8",
                            rand() > 0.9,
                            "m9",
                            rand() > 0.9,
                            "m10",
                            rand() > 0.9,
                            "m11",
                            rand() > 0.9,
                            "m12",
                            rand() > 0.9,
                            "m13",
                            rand() > 0.9,
                            "m14",
                            rand() > 0.9,
                            "m15",
                            rand() > 0.9,
                            "m16",
                            rand() > 0.9,
                            "m17",
                            rand() > 0.9,
                            "m18",
                            rand() > 0.9,
                            "m19",
                            rand() > 0.9,
                            "m20",
                            rand() > 0.9,
                            "m21",
                            rand() > 0.9,
                            "m22",
                            rand() > 0.9,
                            "m23",
                            rand() > 0.9,
                            "m24",
                            rand() > 0.9,
                            "m25",
                            rand() > 0.9,
                            "w1",
                            rand() > 0.9,
                            "w2",
                            rand() > 0.9,
                            "w3",
                            rand() > 0.9,
                            "w4",
                            rand() > 0.9,
                            "w5",
                            rand() > 0.9,
                            "w6",
                            rand() > 0.9,
                            "w7",
                            rand() > 0.9,
                            "w8",
                            rand() > 0.9,
                            "w9",
                            rand() > 0.9,
                            "w10",
                            rand() > 0.9,
                            "w11",
                            rand() > 0.9,
                            "w12",
                            rand() > 0.9,
                            "w13",
                            rand() > 0.9,
                            "w14",
                            rand() > 0.9,
                            "w15",
                            rand() > 0.9,
                            "w16",
                            rand() > 0.9,
                            "w17",
                            rand() > 0.9,
                            "w18",
                            rand() > 0.9,
                            "w19",
                            rand() > 0.9,
                            "w20",
                            rand() > 0.9,
                            "w21",
                            rand() > 0.9,
                            "w22",
                            rand() > 0.9,
                            "w23",
                            rand() > 0.9,
                            "w24",
                            rand() > 0.9,
                            "w25",
                            rand() > 0.9,
                            "x1",
                            rand() > 0.9,
                            "x2",
                            rand() > 0.9,
                            "x3",
                            rand() > 0.9,
                            "x4",
                            rand() > 0.9,
                            "x5",
                            rand() > 0.9,
                            "x6",
                            rand() > 0.9,
                            "x7",
                            rand() > 0.9,
                            "x8",
                            rand() > 0.9,
                            "x9",
                            rand() > 0.9,
                            "x10",
                            rand() > 0.9,
                            "x11",
                            rand() > 0.9,
                            "x12",
                            rand() > 0.9,
                            "x13",
                            rand() > 0.9,
                            "x14",
                            rand() > 0.9,
                            "x15",
                            rand() > 0.9,
                            "x16",
                            rand() > 0.9,
                            "x17",
                            rand() > 0.9,
                            "x18",
                            rand() > 0.9,
                            "x19",
                            rand() > 0.9,
                            "x20",
                            rand() > 0.9,
                            "x21",
                            rand() > 0.9,
                            "x22",
                            rand() > 0.9,
                            "x23",
                            rand() > 0.9,
                            "x24",
                            rand() > 0.9,
                            "x25",
                            rand() > 0.9
                            ),
                "rule2", map(
                            "k1",
                            rand() > 0.9,
                            "k2",
                            rand() > 0.9,
                            "k3",
                            rand() > 0.9,
                            "k4",
                            rand() > 0.9,
                            "k5",
                            rand() > 0.9,
                            "k6",
                            rand() > 0.9,
                            "k7",
                            rand() > 0.9,
                            "k8",
                            rand() > 0.9,
                            "k9",
                            rand() > 0.9,
                            "k10",
                            rand() > 0.9,
                            "k11",
                            rand() > 0.9,
                            "k12",
                            rand() > 0.9
                            ),
                "rule3", map(
                            "k1",
                            rand() > 0.9,
                            "k2",
                            rand() > 0.9,
                            "k3",
                            rand() > 0.9,
                            "k4",
                            rand() > 0.9,
                            "k5",
                            rand() > 0.9,
                            "k6",
                            rand() > 0.9,
                            "k7",
                            rand() > 0.9,
                            "k8",
                            rand() > 0.9,
                            "k9",
                            rand() > 0.9,
                            "k10",
                            rand() > 0.9,
                            "k11",
                            rand() > 0.9,
                            "k12",
                            rand() > 0.9
                            )
             ) AS sigma,
             timestamp,
             value,
             cast(host_id % {conf.numbloom} as string) AS group_key
         from
             events
             join hosts on {join_condition}
     )
     """

    print(sql)
    df = spark.sql(sql)


    # instruct the flux capacitor to cache parent tags
    flux_update_spec = dedent("""
        rules:
            - rulename: rule1
              description: test
              action: parent
              tags:
                - name: k1
                - name: k2
                - name: k3
                - name: k4
                - name: k5
                - name: k6
                - name: k7
                - name: k8
                - name: k9
                - name: k10
                - name: k11
                - name: k12
                - name: k13
                - name: k14
                - name: k15
                - name: k16
                - name: k17
                - name: k18
                - name: k19
                - name: k20
                - name: k21
                - name: k22
                - name: k23
                - name: k24
                - name: k25
                - name: m1
                - name: m2
                - name: m3
                - name: m4
                - name: m5
                - name: m6
                - name: m7
                - name: m8
                - name: m9
                - name: m10
                - name: m11
                - name: m12
                - name: m13
                - name: m14
                - name: m15
                - name: m16
                - name: m17
                - name: m18
                - name: m19
                - name: m20
                - name: m21
                - name: m22
                - name: m23
                - name: m24
                - name: m25
                - name: w1
                - name: w2
                - name: w3
                - name: w4
                - name: w5
                - name: w6
                - name: w7
                - name: w8
                - name: w9
                - name: w10
                - name: w11
                - name: w12
                - name: w13
                - name: w14
                - name: w15
                - name: w16
                - name: w17
                - name: w18
                - name: w19
                - name: w20
                - name: w21
                - name: w22
                - name: w23
                - name: w24
                - name: w25
                - name: x1
                - name: x2
                - name: x3
                - name: x4
                - name: x5
                - name: x6
                - name: x7
                - name: x8
                - name: x9
                - name: x10
                - name: x11
                - name: x12
                - name: x13
                - name: x14
                - name: x15
                - name: x16
                - name: x17
                - name: x18
                - name: x19
                - name: x20
                - name: x21
                - name: x22
                - name: x23
                - name: x24
                - name: x25
              parent: parent_key
              child: key

            - rulename: rule2
              description: proc_creation_win_susp_conhost
              action: parent
              tags:
                - name: pr2_selection
                - name: pr2_filter_git
                - name: k1
                - name: k2
                - name: k3
                - name: k4
                - name: k5
                - name: k6
                - name: k7
                - name: k8
                - name: k9
                - name: k10
                - name: k11
                - name: k12
              parent: parent_key
              child: key

            - rulename: rule3
              description: proc_creation_win_impacket_lateralization
              action: parent
              tags:
                - name: pr3_selection_other
                - name: pr3_selection_atexec
                - name: k1
                - name: k2
                - name: k3
                - name: k4
                - name: k5
                - name: k6
                - name: k7
                - name: k8
                - name: k9
                - name: k10
                - name: k11
                - name: k12
              parent: parent_key
              child: key
            - rulename: rule4
              description: proc_creation_win_impacket_lateralization
              action: parent
              tags:
                - name: a
                - name: b
                - name: c
                - name: d
                - name: e
                - name: f
                - name: g
                - name: h
                - name: i
                - name: j
                - name: k
                - name: l
                - name: m
                - name: n
                - name: o
              parent: parent_key
              child: key
        """)

    print(flux_update_spec)


    df.printSchema()
    df = flux_capacitor(df, flux_update_spec, conf.numfeatures, "group_key")
    df.printSchema()
    
    query = (
        df
        .writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime=f"{conf.trigger} seconds")
        .queryName(conf.name)
        .option("checkpointLocation", checkpoint)
        .toTable(output_table)
    )

    batchId = -1
    while(True):
        time.sleep(1)
        if query.lastProgress:
            if batchId != query.lastProgress['batchId']:
                batchId = query.lastProgress['batchId']
                do_metrics(spark, conf.name, query)
        

def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s [OPTION] [FILE]...",
        description="Description here"
    )
    parser.add_argument('--rate', type=int, required=True)
    parser.add_argument('--partitions', type=int, required=True)
    parser.add_argument('--numhosts', type=int, required=True)
    parser.add_argument('--numfeatures', type=int, required=True)
    parser.add_argument('--numbloom', type=int, required=True)
    parser.add_argument('--trigger', type=int, required=True)
    parser.add_argument('--xmx', type=str, required=False, default="30G")
    parser.add_argument('--store', type=str, required=False, default="rocksdb")
    parser.add_argument('--distribution', type=str, required=False, default="roundrobin")
    parser.add_argument('--catalog', type=str, required=True)
    
    return parser

def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()
    name = f"bloom_r{args.rate}_p{args.partitions}_a{args.numhosts}_b{args.numbloom}_f{args.numfeatures}_t{args.trigger}_x{args.xmx}_{args.store}"
    if args.distribution != "roundrobin":
        name = name + f"_d{args.distribution}"
    print(name)
    args.name = name
    start_query(args)

if __name__ == "__main__":
    sys.exit(main())

    
    
    