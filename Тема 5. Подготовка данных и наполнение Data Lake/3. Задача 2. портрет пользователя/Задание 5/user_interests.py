import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import datetime

import pyspark
from pyspark.sql.window import Window



def main():
    date = sys.argv[1]
    depth  = int(sys.argv[2])
    base_input_path = sys.argv[3]
    base_output_path = sys.argv[4]

    conf = SparkConf().setAppName(f"user_interests-{date}-days{depth}")
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)

    calculate_user_interests(date, depth, spark, base_input_path, base_output_path)
    

def input_event_paths(date, depth, input_path):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"{input_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]


def calculate_user_interests(date, depth, spark, input_path, output_path):
    
    post_tops = tag_tops(date, depth, spark, input_path)
    reaction_tops = reaction_tag_tops(date, depth, spark, input_path)

    merged = post_tops.join(reaction_tops, "user_id", "full_outer").withColumn('date', F.lit(date))
    merged.write.mode("overwrite").parquet(output_path)

    return merged


def tag_tops(date, depth, spark, input_path):
    
    message_paths = input_event_paths(date, depth, input_path)

    result = spark.read\
        .option("basePath", input_path)\
        .parquet(*message_paths)\
        .where("event_type = 'message'")\
        .where("event.message_channel_to is not null")\
        .select(F.col("event.message_id").alias("message_id"),
                F.col("event.message_from").alias("user_id"),
                F.explode(F.col("event.tags")).alias("tag"))\
        .groupBy("user_id", "tag")\
        .agg(F.count("*").alias("tag_count"))\
        .withColumn("rank", F.row_number().over(Window.partitionBy("user_id")\
        .orderBy(F.desc("tag_count"), F.desc("tag"))))\
        .where("rank <= 3")\
        .groupBy("user_id")\
        .pivot("rank", [1, 2, 3])\
        .agg(F.first("tag"))\
        .withColumnRenamed("1", "tag_top_1")\
        .withColumnRenamed("2", "tag_top_2")\
        .withColumnRenamed("3", "tag_top_3")

    return result

def reaction_tag_tops(date, depth, spark, input_path):

    reaction_paths = input_event_paths(date, depth, input_path)

    reactions = spark.read\
        .option("basePath", input_path)\
        .parquet(*reaction_paths)\
        .where("event_type = 'reaction'")

    all_message_tags = spark.read.parquet(input_path)\
        .where("event_type='message' and event.message_channel_to is not null")\
        .select(F.col("event.message_id").alias("message_id"),
                F.col("event.message_from").alias("user_id"),
                F.explode(F.col("event.tags")).alias("tag"))

    reaction_tags = reactions\
        .select(F.col("event.reaction_from").alias("user_id"), 
                F.col("event.message_id").alias("message_id"), 
                F.col("event.reaction_type").alias("reaction_type")
               ).join(all_message_tags.select("message_id", "tag"), "message_id")

    reaction_tops = reaction_tags\
        .groupBy("user_id", "tag", "reaction_type")\
        .agg(F.count("*").alias("tag_count"))\
        .withColumn("rank", F.row_number().over(Window.partitionBy("user_id", "reaction_type")\
        .orderBy(F.desc("tag_count"), F.desc("tag"))))\
        .where("rank <= 3")\
        .groupBy("user_id", "reaction_type")\
        .pivot("rank", [1, 2, 3])\
        .agg(F.first("tag"))\
        .cache()

    like_tops = reaction_tops\
        .where("reaction_type = 'like'")\
        .drop("reaction_type")\
        .withColumnRenamed("1", "like_tag_top_1")\
        .withColumnRenamed("2", "like_tag_top_2")\
        .withColumnRenamed("3", "like_tag_top_3")

    dislike_tops = reaction_tops\
        .where("reaction_type = 'dislike'")\
        .drop("reaction_type")\
        .withColumnRenamed("1", "dislike_tag_top_1")\
        .withColumnRenamed("2", "dislike_tag_top_2")\
        .withColumnRenamed("3", "dislike_tag_top_3")

    result = like_tops.join(dislike_tops, "user_id", "full_outer")

    return result




if __name__ == "__main__":
    main()

# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster 
# /lessons/user_interests.py 
# 2022-05-04 
# 5 
# /user/mustdayker/data/events 
# /user/mustdayker/analytics/user_interests_d5

# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/user_interests.py 2022-05-04 5 /user/mustdayker/data/events /user/mustdayker/analytics/user_interests_d5