import airflow
import os 
# импортируем модуль os, который даёт возможность работы с ОС
# указание os.environ[…] настраивает окружение

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import date, datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# прописываем пути
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8' 

# задаём базовые аргументы
default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'airflow'
}

# вызываем DAG
dag_spark = DAG("task_5-2-5",
          schedule_interval=None,
          catchup=False,
          default_args=default_args
         )

# объявляем задачу с Bash-командой, которая распечатывает дату
event_reader = SparkSubmitOperator(
                        task_id='event_reader',
                        dag=dag_spark,
                        application ='/lessons/partition.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ['2022-05-31', '/user/master/data/events', '/user/mustdayker/data/events'],
                        conf={"spark.driver.maxResultSize": "20g"},
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

partition_7_write = SparkSubmitOperator(
                        task_id='partition_7_write',
                        dag=dag_spark,
                        application ='/lessons/verified_tags_candidates.py',
                        conn_id= 'yarn_spark',
                        application_args = ['2022-05-31', 
                                            '7', 
                                            '300', 
                                            '/user/mustdayker/data/events', 
                                            '/user/master/data/snapshots/tags_verified/actual', 
                                            '/user/mustdayker/5.2.5/analytics/verified_tags_candidates_d7'],
                        conf={"spark.driver.maxResultSize": "20g"},
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

partition_84_write = SparkSubmitOperator(
                        task_id='partition_84_write',
                        dag=dag_spark,
                        application ='/lessons/verified_tags_candidates.py',
                        conn_id= 'yarn_spark',
                        application_args = ['2022-05-31', 
                                            '84', 
                                            '300', 
                                            '/user/mustdayker/data/events', 
                                            '/user/master/data/snapshots/tags_verified/actual', 
                                            '/user/mustdayker/5.2.5/analytics/verified_tags_candidates_d84'],
                        conf={"spark.driver.maxResultSize": "20g"},
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

event_reader >> [partition_7_write, partition_84_write]
