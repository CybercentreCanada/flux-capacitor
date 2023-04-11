import time
import sys
import time

from demo.constants import init_argparse
import demo.constants as constants
from demo.util import (
    get_checkpoint_location,
    get_spark,
    create_spark_session,
    create_view,
    make_name,
    monitor_query,
    print_anomalies,
    render_statement,
    validate_events,
    run
)

def run():
    print('inside run')
    create_spark_session("streaming alert builder", 1)
    time.sleep(15)
    raise Exception("this is an exception")

run()