
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


def flux_capacitor(input_df, spec, bloom_capacity, group_col, store):
    """Python function to invoke the scala code"""
    spark = SparkSession.builder.getOrCreate()
    flux_stateful_function = spark._sc._jvm.cccs.fluxcapacitor.FluxCapacitor.invoke
    use_fluxstore = store == "fluxstore" or store == "fluxstorecomp"
    jdf = flux_stateful_function(
            input_df._jdf, 
            group_col, 
            bloom_capacity, 
            spec,
            use_fluxstore)
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
    NUMRULES = 10

    rules = dedent("""rules:
    """)
    for rule in range(1, NUMRULES):
        rules += dedent(f"""
            - rulename: rule{rule}
              description: test
              action: parent
              parent: parent_key
              child: key
              tags:
                - name: filter_msiexec_syswow1
                - name: filter_msiexec_syswow2
                - name: filter_msiexec_syswow3
                - name: filter_msiexec_syswow4
                - name: filter_msiexec_syswow5
                """)

    # instruct the flux capacitor to cache parent tags
    flux_update_spec = rules

    print(flux_update_spec)



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

    sigmaTags = ""
    for rule in range(1, NUMRULES):
        sigmaTags += f"""
            "rule{rule}", map("filter_msiexec_syswow1",
                            rand() > 0.9,
                            "filter_msiexec_syswow2",
                            rand() > 0.9,
                            "filter_msiexec_syswow3",
                            rand() > 0.9,
                            "filter_msiexec_syswow4",
                            rand() > 0.9,
                            "filter_msiexec_syswow5",
                            rand() > 0.9,
                            "filter_msiexec_syswow6",
                            rand() > 0.9,
                            "filter_msiexec_syswow7",
                            rand() > 0.9,
                            "filter_msiexec_syswow8",
                            rand() > 0.9,
                            "filter_msiexec_syswow9",
                            rand() > 0.9,
                            "filter_msiexec_syswow10",
                            rand() > 0.9
                            ),""" 
    sigmaTags = sigmaTags[:-1]

    sql = f"""
    select * from (
         select
             map('rule1', map('tag1', TRUE)) as sigma,
             timestamp,
             --value,
             cast(1 % {conf.numbloom} as string) AS group_key
         from
             events
     )
     """

    print(sql)

    
    useFluxStateStore = False
    useFluxStateStoreCompression = False
    if conf.store == "fluxstore":
      useFluxStateStore = True
      useFluxStateStoreCompression = False
    
    if conf.store == "fluxstorecomp":
      useFluxStateStore = True
      useFluxStateStoreCompression = True

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
            .config("spark.cccs.fluxcapacitor.compression", useFluxStateStoreCompression)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )
    
    if conf.store == 'rocksdb':
        builder = (builder
            .config("spark.sql.streaming.stateStore.providerClass", 
                    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
                   )
    if useFluxStateStore:
        builder = (builder
            .config("spark.sql.streaming.stateStore.providerClass", 
                    "org.apache.spark.sql.execution.streaming.state.FluxStateStoreProvider")
                   )
    
    spark = builder.getOrCreate()
    
    spark.udf.register("executor_metrics", executor_metrics, executor_metrics_schema)

    # hosts = spark.range(conf.numhosts).withColumn('a_uuid', F.expr('uuid()')).withColumnRenamed('id', 'host_id')

    # hosts.writeTo(f'experiment.jcc.host_table').createOrReplace()

    # hosts = spark.table(f'experiment.jcc.host_table')

    # hosts.persist()
    # hosts.createOrReplaceTempView("hosts")
    # hosts.show()
    # print(hosts.count())
    
    # df = (
    #     spark.readStream
    #     .format("rate-micro-batch")
    #     .option("rowsPerBatch", conf.rate * conf.trigger)
    #     .load()
    #     .withWatermark("timestamp", "0 seconds")
    #     .withColumn("name", F.lit(conf.name))
    # )

    print(conf.rate)
    df = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", conf.rate)
        .load()
        #.withWatermark("timestamp", "0 seconds")
        .withColumn("name", F.lit(conf.name))
    )
    
    df.createOrReplaceTempView("events")
    

    df = spark.sql(sql)



    df.printSchema()
    df = flux_capacitor(df, flux_update_spec, conf.numfeatures, "group_key", conf.store)
    df.printSchema()
    
    #df = df.filter(F.expr("value % 10000 = 0"))

    query = (
        df
        .writeStream
        .format("console")
        .outputMode("append")
        .trigger(continuous=f"{conf.trigger} seconds")
        .queryName(conf.name)
        .option("checkpointLocation", checkpoint)
        .start()
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
    parser.add_argument('--prefix', type=str, required=False, default="bloom")
    
    return parser

def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()
    name = f"{args.prefix}_r{args.rate}_p{args.partitions}_a{args.numhosts}_b{args.numbloom}_f{args.numfeatures}_t{args.trigger}_x{args.xmx}_{args.store}"
    if args.distribution != "roundrobin":
        name = name + f"_d{args.distribution}"
    print(name)
    args.name = name
    start_query(args)

if __name__ == "__main__":
    sys.exit(main())

    
    
    