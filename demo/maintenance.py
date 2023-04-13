from datetime import datetime, timedelta
from demo.constants import init_globals
import demo.constants as constants
from demo.util import (
    get_spark,
    create_spark_session,
    get_metadata_location,
    get_data_location
)

import logging as logging
log = logging.getLogger(__name__)

def prev_hour():
    prev_hour_ts = datetime.today() - timedelta(hours=1)
    prev_hour = prev_hour_ts.strftime("%Y-%m-%d %H:%M:%S")
    return prev_hour

def prev_day():
    prev_day_dt = datetime.today() - timedelta(days=1)
    prev_day = prev_day_dt.strftime("%Y-%m-%d %H:%M:%S")
    return prev_day

def prev_day_partition():
    prev_day_dt = datetime.today() - timedelta(days=1)
    prev_day = prev_day_dt.strftime("%Y-%m-%d 00:00:00")
    return prev_day

def prev_week_partition():
    prev_day_dt = datetime.today() - timedelta(days=7)
    prev_day = prev_day_dt.strftime("%Y-%m-%d 00:00:00")
    return prev_day

def today_partition():
    today_dt = datetime.today()
    today = today_dt.strftime("%Y-%m-%d 00:00:00")
    return today

def expire_snapshot_of_table(table_name):
    log.info(f"expire_snapshot_of_table of {table_name}")
    sql = f"""
        CALL {constants.catalog}.system.expire_snapshots(
                '{table_name}',
                timestamp '{prev_week_partition()}'
        )
    """
    log.info(sql)
    get_spark().sql(sql).show()


def remove_orphan_files_of_table(table_name):
    log.info(f"remove_orphan_files of {table_name}")
    data_location = get_data_location(table_name)
    sql = f"""
    CALL {constants.catalog}.system.remove_orphan_files(
            table => '{table_name}',
            location => '{data_location}',
            older_than => timestamp '{prev_day()}',
            max_concurrent_deletes => 50,
            dry_run => false
        )
    """
    log.info(sql)
    get_spark().sql(sql).show(truncate=False)

    metadata_location = get_metadata_location(table_name)
    sql = f"""
    CALL {constants.catalog}.system.remove_orphan_files(
            table => '{table_name}',
            location => '{metadata_location}',
            older_than => timestamp '{prev_day()}',
            max_concurrent_deletes => 50,
            dry_run => false
        )
    """
    log.info(sql)
    get_spark().sql(sql).show(truncate=False)




def sort_latest_files_in_current_partition_of_tagged_telemetry_table():
    log.info("sort_latest_files_in_current_partition_of_tagged_telemetry_table")
    sql = f"""
    CALL {constants.catalog}.system.rewrite_data_files(
            table => '{constants.tagged_telemetry_table}',
            strategy => 'sort',
            sort_order => 'host_id, has_temporal_proximity_tags',
            options => map('min-input-files', '100',
                        'max-concurrent-file-group-rewrites', '30',
                        'partial-progress.enabled', 'true'),
            where => 'timestamp >= TIMESTAMP \\'{today_partition()}\\' '
        )
    """
    log.info(sql)
    get_spark().sql(sql).show()

def sort_full_day_of_tagged_telemetry_table():
    log.info("sort_full_day_of_tagged_telemetry_table")
    sql = f"""
    CALL {constants.catalog}.system.rewrite_data_files(
            table => '{constants.tagged_telemetry_table}',
            strategy => 'sort',
            sort_order => 'host_id, has_temporal_proximity_tags',
            options => map('min-input-files', '100',
                        'max-concurrent-file-group-rewrites', '30',
                        'partial-progress.enabled', 'true',
                        'rewrite-all', 'true'),
            where => 'timestamp >= TIMESTAMP \\'{prev_day_partition()}\\' AND timestamp < TIMESTAMP \\'{today_partition()}\\' '
        )
    """
    log.info(sql)
    get_spark().sql(sql).show()

def binpack_full_day_of_metrics_table():
    log.info("binpack_full_day_of_metrics_table")
    sql = f"""
    CALL {constants.catalog}.system.rewrite_data_files(
            table => '{constants.metrics_table}',
            options => map('min-input-files', '100',
                        'max-concurrent-file-group-rewrites', '100',
                        'partial-progress.enabled', 'false',
                        'rewrite-all', 'true'),
            where => 'timestamp >= TIMESTAMP \\'{prev_day_partition()}\\' AND timestamp < TIMESTAMP \\'{today_partition()}\\' '
        )
    """
    log.info(sql)
    get_spark().sql(sql).show()

def ageoff_process_telemetry_table():
    log.info("ageoff_process_telemetry_table")
    sql = f"""
        delete
        from
            {constants.process_telemetry_table}
        where
            timestamp < '{prev_week_partition()}'
    """
    log.info(sql)
    get_spark().sql(sql).show()

def every_hour(catalog, schema, verbose):
    init_globals(catalog, schema, verbose)
    create_spark_session("every_hour", num_machines=1, driver_mem="2g")
    try:
        sort_latest_files_in_current_partition_of_tagged_telemetry_table()
    finally:
        get_spark().stop()
        log.info("done")

def every_day(catalog, schema, verbose):
    init_globals(catalog, schema, verbose)
    create_spark_session("every_day", num_machines=1, driver_mem="2g")
    try:
        ageoff_process_telemetry_table()
        sort_full_day_of_tagged_telemetry_table()
        binpack_full_day_of_metrics_table()
        expire_snapshot_of_table(constants.alerts_table)
        expire_snapshot_of_table(constants.process_telemetry_table)
        expire_snapshot_of_table(constants.suspected_anomalies_table)
        expire_snapshot_of_table(constants.tagged_telemetry_table)
        expire_snapshot_of_table(constants.metrics_table)
        remove_orphan_files_of_table(constants.alerts_table)
        remove_orphan_files_of_table(constants.process_telemetry_table)
        remove_orphan_files_of_table(constants.suspected_anomalies_table)
        remove_orphan_files_of_table(constants.tagged_telemetry_table)
        remove_orphan_files_of_table(constants.metrics_table)
    finally:
        get_spark().stop()
        log.info("done")


