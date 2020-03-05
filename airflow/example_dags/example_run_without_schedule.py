# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

dag = DAG(dag_id='example_run_without_schedule', start_date=days_ago(2))
with dag:
    run_this_first = DummyOperator(task_id='run_this_first')

    run_this_last = BashOperator(task_id='run_this_last', bash_command='echo 1 {{ dag.dag_id }}')

    run_this_last_1 = BashOperator(
        task_id='run_this_last_1',
        bash_command='echo 1 {{ task_instance_key_str }} {{params.x}} - {{next_execution_date}}',
        params={"x": "y"})

    run_this_last_2 = SimpleHttpOperator(task_id='run_this_last_2', endpoint="/get", method='GET')

    def xxx(**context):
        print(context["dag"].dag_id)
        return "xxxxxx"

    run_python_op = PythonOperator(task_id='run_python_op', python_callable=xxx)

    run_this_first >> run_this_last >> run_this_last_1 >> run_python_op >> run_this_last_2

if __name__ == "__main__":
    dag.run_without_schedule(
        mock_variables={"paa": "paas"},
        environment_variables={"AIRFLOW_CONN_HTTP_DEFAULT": "https://httpbin.org/"}
    )
