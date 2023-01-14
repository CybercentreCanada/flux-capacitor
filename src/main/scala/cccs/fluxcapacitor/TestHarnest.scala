package cccs.fluxcapacitor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.Trigger

case class Config(rate:Int, partitions:Int, numhosts:Int, numfeatures:Int, numbloom:Int, trigger:Int, xmx:String="2G", store:String="fluxstore", distribution:String="roundrobin", catalog:String){
    val name = s"bloom_r${rate}_p${partitions}_a${numhosts}_b${numbloom}_f${numfeatures}_t${trigger}_x${xmx}_${store}"
}



object TestHarnest {
  def main(args: Array[String]) = {

        println("Hello, world")


    var conf = Config(rate=1, partitions=1, numhosts=1, numfeatures=100000, numbloom=1, trigger=2, xmx="2G", store="fluxstore", catalog="file:///tmp/iceberg")



    var output_table = s"experiment.jcc.${conf.name}"
    var checkpoint = s"${conf.catalog}/jcc/${conf.name}/checkpoint/"

    var useFluxStateStore = false
    var useFluxStateStoreCompression = false
    if (conf.store == "fluxstore"){
      useFluxStateStore = true
      useFluxStateStoreCompression = false
    }
    else if (conf.store == "fluxstorecomp"){
      useFluxStateStore = true
      useFluxStateStoreCompression = true
    }

    var builder = (
        SparkSession.builder
            .appName(conf.name)
            .master("local")
            .config("spark.sql.shuffle.partitions", conf.partitions)
            .config("spark.driver.cores", 1)
            .config("spark.driver.memory", "2G")
            .config("spark.executor.memory", conf.xmx)
            .config("spark.cores.max", 16)
            .config("spark.executor.cores", 16)
            .config("spark.sql.catalog.experiment", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.experiment.type", "hadoop")
            .config("spark.sql.catalog.experiment.warehouse", conf.catalog)
            .config("spark.jars", "./target/flux-capacitor-1.jar")
            .config("spark.sql.streaming.maxBatchesToRetainInMemory", 1)
            .config("spark.cccs.fluxcapacitor.compression", useFluxStateStoreCompression )
    )
    
    if (useFluxStateStore){
    builder = (builder
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.FluxStateStoreProvider")
                )
    }

    var spark = builder.getOrCreate()
    
    //spark.udf.register("executor_metrics", executor_metrics, executor_metrics_schema)

    var hosts = spark.range(conf.numhosts).withColumn("a_uuid", expr("uuid()")).withColumnRenamed("id", "host_id")
    hosts.persist()
    hosts.createOrReplaceTempView("hosts")
    hosts.show()
    print(hosts.count())
    
    var df = (
        spark.readStream
        .format("rate-micro-batch")
        .option("rowsPerBatch", conf.rate * conf.trigger)
        .load()
        .withWatermark("timestamp", "0 seconds")
        .withColumn("name", lit(conf.name))
    )

    df.createOrReplaceTempView("events")
    
    var join_condition = if (conf.distribution == "roundrobin")
                            s"value % ${conf.numhosts} = host_id"
                        else{
                            // generations 
                            // at a rate of r5000 and a trigger of 5min t300
                            // we have 1.5M events per micro-batch
                            // 1.5M / 50k hosts = 30
                            // instead of using modulo like in round robin we will divide
                            // this simulates getting telemery from 30 machines, per micro-batch
                            s"((cast(value / 300000 as int) * 1000) + (value % 1000)) % ${conf.numhosts} = id"
                        }
    print(join_condition)
    
    var sql = s"""
    select *, sigma as sigma_input from (
         select
             a_uuid,
             a_uuid || "_" || value || "_" || "childkey" AS key,
             a_uuid || "_" || value || "_" || "parentkey" AS parent_key,
             map(
                "rule1", map(
                            "k1",
                            rand() > 0.5,
                            "k2",
                            rand() > 0.5,
                            "k3",
                            rand() > 0.5,
                            "k4",
                            rand() > 0.5,
                            "k5",
                            rand() > 0.5,
                            "k6",
                            rand() > 0.5,
                            "k7",
                            rand() > 0.5,
                            "k8",
                            rand() > 0.5,
                            "k9",
                            rand() > 0.5,
                            "k10",
                            rand() > 0.5,
                            "k11",
                            rand() > 0.5,
                            "k12",
                            rand() > 0.5,
                            "k13",
                            rand() > 0.5,
                            "k14",
                            rand() > 0.5,
                            "k15",
                            rand() > 0.5,
                            "k16",
                            rand() > 0.5,
                            "k17",
                            rand() > 0.5,
                            "k18",
                            rand() > 0.5,
                            "k19",
                            rand() > 0.5,
                            "k20",
                            rand() > 0.5,
                            "k21",
                            rand() > 0.5,
                            "k22",
                            rand() > 0.5,
                            "k23",
                            rand() > 0.5,
                            "k24",
                            rand() > 0.5,
                            "k25",
                            rand() > 0.5,
                            "m1",
                            rand() > 0.5,
                            "m2",
                            rand() > 0.5,
                            "m3",
                            rand() > 0.5,
                            "m4",
                            rand() > 0.5,
                            "m5",
                            rand() > 0.5,
                            "m6",
                            rand() > 0.5,
                            "m7",
                            rand() > 0.5,
                            "m8",
                            rand() > 0.5,
                            "m9",
                            rand() > 0.5,
                            "m10",
                            rand() > 0.5,
                            "m11",
                            rand() > 0.5,
                            "m12",
                            rand() > 0.5,
                            "m13",
                            rand() > 0.5,
                            "m14",
                            rand() > 0.5,
                            "m15",
                            rand() > 0.5,
                            "m16",
                            rand() > 0.5,
                            "m17",
                            rand() > 0.5,
                            "m18",
                            rand() > 0.5,
                            "m19",
                            rand() > 0.5,
                            "m20",
                            rand() > 0.5,
                            "m21",
                            rand() > 0.5,
                            "m22",
                            rand() > 0.5,
                            "m23",
                            rand() > 0.5,
                            "m24",
                            rand() > 0.5,
                            "m25",
                            rand() > 0.5,
                            "w1",
                            rand() > 0.5,
                            "w2",
                            rand() > 0.5,
                            "w3",
                            rand() > 0.5,
                            "w4",
                            rand() > 0.5,
                            "w5",
                            rand() > 0.5,
                            "w6",
                            rand() > 0.5,
                            "w7",
                            rand() > 0.5,
                            "w8",
                            rand() > 0.5,
                            "w9",
                            rand() > 0.5,
                            "w10",
                            rand() > 0.5,
                            "w11",
                            rand() > 0.5,
                            "w12",
                            rand() > 0.5,
                            "w13",
                            rand() > 0.5,
                            "w14",
                            rand() > 0.5,
                            "w15",
                            rand() > 0.5,
                            "w16",
                            rand() > 0.5,
                            "w17",
                            rand() > 0.5,
                            "w18",
                            rand() > 0.5,
                            "w19",
                            rand() > 0.5,
                            "w20",
                            rand() > 0.5,
                            "w21",
                            rand() > 0.5,
                            "w22",
                            rand() > 0.5,
                            "w23",
                            rand() > 0.5,
                            "w24",
                            rand() > 0.5,
                            "w25",
                            rand() > 0.5,
                            "x1",
                            rand() > 0.5,
                            "x2",
                            rand() > 0.5,
                            "x3",
                            rand() > 0.5,
                            "x4",
                            rand() > 0.5,
                            "x5",
                            rand() > 0.5,
                            "x6",
                            rand() > 0.5,
                            "x7",
                            rand() > 0.5,
                            "x8",
                            rand() > 0.5,
                            "x9",
                            rand() > 0.5,
                            "x10",
                            rand() > 0.5,
                            "x11",
                            rand() > 0.5,
                            "x12",
                            rand() > 0.5,
                            "x13",
                            rand() > 0.5,
                            "x14",
                            rand() > 0.5,
                            "x15",
                            rand() > 0.5,
                            "x16",
                            rand() > 0.5,
                            "x17",
                            rand() > 0.5,
                            "x18",
                            rand() > 0.5,
                            "x19",
                            rand() > 0.5,
                            "x20",
                            rand() > 0.5,
                            "x21",
                            rand() > 0.5,
                            "x22",
                            rand() > 0.5,
                            "x23",
                            rand() > 0.5,
                            "x24",
                            rand() > 0.5,
                            "x25",
                            rand() > 0.5
                            ),
                "rule2", map(
                            "k1",
                            rand() > 0.5,
                            "k2",
                            rand() > 0.5,
                            "k3",
                            rand() > 0.5,
                            "k4",
                            rand() > 0.5,
                            "k5",
                            rand() > 0.5,
                            "k6",
                            rand() > 0.5,
                            "k7",
                            rand() > 0.5,
                            "k8",
                            rand() > 0.5,
                            "k9",
                            rand() > 0.5,
                            "k10",
                            rand() > 0.5,
                            "k11",
                            rand() > 0.5,
                            "k12",
                            rand() > 0.5
                            ),
                "rule3", map(
                            "k1",
                            rand() > 0.5,
                            "k2",
                            rand() > 0.5,
                            "k3",
                            rand() > 0.5,
                            "k4",
                            rand() > 0.5,
                            "k5",
                            rand() > 0.5,
                            "k6",
                            rand() > 0.5,
                            "k7",
                            rand() > 0.5,
                            "k8",
                            rand() > 0.5,
                            "k9",
                            rand() > 0.5,
                            "k10",
                            rand() > 0.5,
                            "k11",
                            rand() > 0.5,
                            "k12",
                            rand() > 0.5
                            )
             ) AS sigma,
             timestamp,
             value,
             cast(host_id % ${conf.numbloom} as string) AS group_key
         from
             events
             join hosts on ${join_condition}
     )
     """

    println(sql)
    df = spark.sql(sql)


    // instruct the flux capacitor to cache parent tags
    var flux_update_spec = (s"""
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

    println(flux_update_spec)


    df.printSchema()
    df = FluxCapacitor.invoke(df, "group_key", conf.numfeatures, flux_update_spec, useFluxStateStore)
    df.printSchema()
    
    var query = (
        df
        .writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime(s"${conf.trigger} seconds"))
        .queryName(conf.name)
        .option("checkpointLocation", checkpoint)
        .toTable(output_table)
    )

    query.awaitTermination()
    }
}
