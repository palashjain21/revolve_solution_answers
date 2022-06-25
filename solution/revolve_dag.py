import logging
from datetime import timedelta, datetime

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import (
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import (
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

__VERSION__ = "1.0.0"

DAG_ID = f"Solution_Start_DAG-v{__VERSION__}"
DAG_DESCRIPTION = "The complete pipeline for creating customer patterns"
DATE = "{{ ds }}"

LOGGER = logging.getLogger(DAG_ID)

JOB_FLOW_OVERRIDES = {
    "Name": f"create_customer_patterns_{DATE}",
    "LogUri": "s3://logs.concirrus.com/airflow/",
    "ReleaseLabel": "emr-6.2.0",
    "JobFlowRole": "EMR_S3",
    "ServiceRole": "EMR_DefaultRole",
    "Instances": {
        "Ec2KeyName": "",
        "Ec2SubnetId": "",
        "InstanceGroups": [
            {
                "Name": "Master Instance Group",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.2xlarge",
                "InstanceCount": 1,
            },
        ],
        "TerminationProtected": False,
        "KeepJobFlowAliveWhenNoSteps": False,
    },
    "Applications": [{"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark",
            "Properties": {"maximizeResourceAllocation": "false"},
        },
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3",
                    },
                }
            ],
            "Properties": {},
        },
    ],
    "BootstrapActions": [
        {
            "Name": "Install Python Modules",
            "ScriptBootstrapAction": {
                "Path": "s3://bucket/bootstrap.sh",
            },
        },
    ],
    "VisibleToAllUsers": True,
    "SecurityConfiguration": "EMRFS_Roles_Security_Configuration",
    "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
    "Tags": [
        {"Key": "application", "Value": "airflow"},
        {"Key": "job", "Value": "spoofing_cleaning"},
    ],
}

SPARK_STEPS = [
    {
        "Name": "setup - copy files",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "aws",
                "s3",
                "cp",
                "--recursive",
                "~/solution/solution_start.py",
                "/home/hadoop/",
            ],
        },
    },
]


def task_fail_slack_alert(context):
    base_url = "slack_url"
    log_url = context.get("task_instance").log_url
    slack_webhook_token = BaseHook.get_connection("slack_channel").password
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag}
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        exec_date=context.get("execution_date"),
        log_url=log_url.replace("http://localhost:8080/", base_url),
    )
    failed_alert = SlackWebhookOperator(
        task_id="slack_fail",
        http_conn_id="slack_channel",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
        dag=dag,
    )
    return failed_alert.execute(context=context)


default_args = {
    "owner": "PalashJain",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "start_date": datetime(2022, 6, 25),
    "email": ["xyz_gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=15),
    "on_failure_callback": task_fail_slack_alert,
}

dag = DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    schedule_interval="0 0 * * 0",
    start_date=datetime(2022, 6, 25),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
)

start_daily_pipeline = DummyOperator(
    task_id="start_pipeline", dag=dag
)

cluster_creator = EmrCreateJobFlowOperator(
    task_id="create_job_flow",
    aws_conn_id="aws_role_cdp-emr",
    emr_conn_id="emr_default",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    dag=dag,
)

step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id="aws_role_cdp-emr",
    steps=SPARK_STEPS,
    dag=dag,
)

step1_checker = EmrStepSensor(
    task_id="watch_step_copy_files_from_s3",
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id="aws_role_cdp-emr",
    dag=dag,
)

step2_checker = EmrStepSensor(
    task_id="watch_step_transform",
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[1] }}",
    aws_conn_id="aws_role_cdp-emr",
    dag=dag,
)

job_flow_checker = EmrJobFlowSensor(
    task_id="watch_job_flow",
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id="aws_role_cdp-emr",
    dag=dag,
)

cluster_remover = EmrTerminateJobFlowOperator(
    task_id="remove_cluster",
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id="aws_role_cdp-emr",
    dag=dag,
)

start_daily_pipeline >> cluster_creator >> step_adder
step_adder >> [step1_checker] >> job_flow_checker >> cluster_remover
