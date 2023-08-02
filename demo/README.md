## Integration Testing

At the root of the git repo run this command to run an integration test

```bash
python -m demo.integration_test --catalog hogwarts_users_u_ouo --schema jcc_integration --trigger 60 --verbose true
```

## Launching Streaming Jobs

The streaming demo consists of the following jobs

The `demo/run_streaming_apps.sh` script creates the necessary tables and starts the streaming jobs. The first time you will want to pass the `reset` argument to create new tables

```bash
./demo/run_streaming_apps.sh reset
```

It will start the pyspark process in the background. You can see them running

```bash
ps auxww | grep 'demo\.'
```

Outputs are written to the `./logs/` folder.


## Scheduling Streaming Jobs

You can also just create the tables before running the script or schedule the jobs

```bash
python -m demo.create_tables --catalog hogwarts_users_u_ouo --schema jc_sched
```

## Table Maintenance of Streaming Jobs

When running streaming jobs for long period of time, Iceberg table will require maintenance. This is explained here https://iceberg.apache.org/docs/latest/spark-structured-streaming/

Every hour we run the function `every_hour` in this script `demo/maintenance.py`

You can test out the maintenance script locally using this command

```bash
python -m demo.maintenance --catalog hogwarts_users_u_ouo --schema jc_sched --verbose true
```
