import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

import datetime

# os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

# import findspark
# findspark.init()
# findspark.find()

def main():
    date = sys.argv[1]
    depth  = sys.argv[2]
    user_limit = sys.argv[3]
    data_path = sys.argv[4]
    tags_verified_path = sys.argv[5]
    target_path = sys.argv[6] 

    conf = SparkConf().setAppName(f"EventsPartitioningJob-{date}-d{depth}-cut{user_limit}")
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)

    paths = input_paths(date, depth, data_path)
    messages = spark.read.parquet(*paths)

    all_tags = messages.where("event.message_channel_to is not null")\
        .selectExpr(["event.message_from as user", "explode(event.tags) as tag"])\
        .groupBy("tag")\
        .agg(F.expr("count(distinct user) as suggested_count"))\
        .where(f"suggested_count >= {user_limit}")


    verified_tags = spark.read.parquet(tags_verified_path)
    candidates = all_tags.join(verified_tags, "tag", "left_anti")
    candidates.write.mode("overwrite").parquet(target_path + "/date=" + date)

def input_paths(date, depth, data_path):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"{data_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message" for x in range(depth)]



if __name__ == "__main__":
    main()

# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/verified_tags_candidates.py 2022-05-31 5 300 /user/mustdayker/data/events /user/master/data/snapshots/tags_verified/actual /user/mustdayker/5.2.4/analytics/verified_tags_candidates_d5