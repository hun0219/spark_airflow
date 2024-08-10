# The DAG object; we'll need this to instantiate a DAG

from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent

# Operators; we need this to operate!

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
    PythonVirtualenvOperator,
    BranchPythonOperator,
)

from pyspark_airflow.repartition import repartition, rm_dir


with DAG(
    'pyspark_movie',
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='spark movie DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 1, 5),
    catchup=True,
    tags=['spark'],
) as dag:



    def repartition(ds_nodash):
        from pyspark_airflow.repartition import repartition, rm_dir
        df_row_cnt, read_path, write_path = repartition(ds_nodash)
        print(f'df_row_cnt:{df_row_cnt}')
        print(f'read_path:{read_path}')
        print(f'write_path:{write_path}')


##########################################################################

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")



    re_partition = PythonVirtualenvOperator(
            task_id='re.partition',
            python_callable=repartition,
            requirements=["git+https://github.com/hun0219/spark_airflow.git@v0.1.0"],
            system_site_packages=False,
    )


    join_df = BashOperator(
            task_id='join.df',
            bash_command="""
            $SPARK_HOME/bin/spark-submit /home/hun/airflow_pyspark/movie_join_df.py {{ds_nodash}}
            """
    )
    
    agg = BashOperator(
            task_id='agg',
            bash_command="""
            $SPARK_HOME/bin/spark-submit /home/hun/airflow_pyspark/movie_sum.py {{ds_nodash}}
            """
    )

#    rm_dir = BashOperator(
#            task_id='rm.dir',
#            bash_command='rm -rf ~/data/movie/repartition/load_dt={{ds_nodash}}'
#    )
#


################################################3

    start >> re_partition >> join_df >> agg >> end
