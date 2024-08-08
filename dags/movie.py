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
start_date=datetime(2020, 1, 1),
end_date=datetime(2020, 1, 1),
catchup=True,
tags=['spark'],
) as dag:



    def repartition(ds_nodash):
        from pyspark_airflow.repartition import repartition
        df = repartition(load_dt, from_path=arg1):




##########################################################################

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")

    re_partition = PythonVirtualenvOperator(
            task_id='re.partition',
            python_callable=fun_ext,
            requirements=["git+https://github.com/play-gogo/Extract.git@d2/0.1.0"],
            system_site_packages=False,
            op_args=["{{ds_nodash}}"]
    )


    join_df = BashOperator(
            task_id='join.df',
            bash_command="echo 'task'"
    )
    
    agg = BashOperator(
            task_id='agg',
            bash_command="echo 'task'"



################################################3

    start >> re_partition >> join_df >> agg >>end
