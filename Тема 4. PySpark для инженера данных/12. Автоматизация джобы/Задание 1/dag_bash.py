import airflow
import os 
# импортируем модуль os, который даёт возможность работы с ОС
# указание os.environ[…] настраивает окружение

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import date, datetime

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
dag = DAG("bash_dag",
          schedule_interval=None,
          catchup=False,
          default_args=default_args
         )

# объявляем задачу с Bash-командой, которая распечатывает дату
t1 = BashOperator(
    task_id='bash_operator',
    bash_command='/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/partition.py 2022-05-31 /user/master/data/events /user/mustdayker/data/events',
    retries=3,
    dag=dag
)

t1 