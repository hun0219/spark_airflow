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


    def check_fun(ds_nodash):
        import os
        from pyspark_airflow.repartition import repartition, rm_dir
        rm = rm_dir(dir_path)
        #ld = kwargs['ds_nodash']
        #OS의 경로 가져오는 방법
        home_dir = os.path.expanduser("~")
        path = f'{home_dir}/data/movie/repartition/load_dt={ds_nodash}'
        #path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ld}")
        if os.path.exists(path):
            return "rm.dir" #task_id #rm_dor.task_id 도 가능
        else:
            return "repartition" #task_id


##########################################################################

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")


    check_op = BranchPythonOperator(
            task_id='check.op',
            python_callable=check_fun
            )


    re_partition = PythonVirtualenvOperator(
            task_id='re.partition',
            python_callable=repartition,
            requirements=["git+https://github.com/hun0219/spark_airflow.git@v0.1.0"],
            system_site_packages=False,
    )


    join_df = BashOperator(
            task_id='join.df',
            bash_command="""
            $SPARK_HOME/bin/spark-submit /home/hun/airflow_pyspark.py {{ds_nodash}}
            """
    )
    
    agg = BashOperator(
            task_id='agg',
            bash_command="""
            echo "{{ds_nodash}}"
            """
    )

    rm_dir = BashOperator(
            task_id='rm.dir',
            bash_command='rm -rf ~/data/movie/repartition/load_dt={{ds_nodash}}'
    )



################################################3

    start >> check_op >> [rm_dir,re_partition] >> join_df >> agg >> end
