import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

date = '2022-01-01'
base_input_path = '/user/master/data/events'
base_output_path = '/user/mustdayker/data/events'


def main():
    date = sys.argv[1]
    base_input_path = sys.argv[2]
    base_output_path = sys.argv[3]

    conf = SparkConf().setAppName(f"EventsPartitioningJob-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    events = sql.read.json(f"{base_input_path}/date={date}")
    writer = partition_writer(events)
    writer.save(f'{base_output_path}/date={date}')

def partition_writer(events):
     return events \
        .write \
        .mode('overwrite') \
        .partitionBy('event_type') \
        .format('parquet')

if __name__ == "__main__":
        main()
