
import os
from typing import Optional
from datetime import datetime, timedelta
from airflow.models.baseoperator import BaseOperator
from airflow.configuration import conf
from utils.os_helper import clear_fs_obj
from utils.common import println, timing

class AirflowCleanLogsOperator(BaseOperator):
    """
    A operator to clean logs by retain days
    :param run_date: the execute_date of dagrun with format '%Y-%m-%d' (templated)
    :type run_date: str

    :param system_retain_days: the number of days until the run_date should be keep system logs (templated)
    :type system_retain_days: int

    :param user_retain_days: the number of days until the run_date should be keep user defined dag logs (templated)
    :type user_retain_days: int

    :param ssh_conn_id: the ssh connection information for ssh to all airflow component node
    :type ssh_conn_id: str

    :param base_log_folder: the root folder to store logs
    :type base_log_folder: Optional[str]
    """

    template_fields = ('system_retain_days', 'user_retain_days', 'run_date', 'base_log_folder')
    ui_color = '#64d97d'

    def __init__(self,
            run_date: str,
            system_retain_days: int = 3,
            user_retain_days: int = 15,
            base_log_folder: Optional[str] = None,
            ssh_conn_id: str = 'ssh_airflow_default',
            *args, **kwargs):
        super(AirflowCleanLogsOperator, self).__init__(*args, **kwargs)
        self.run_date = run_date
        self.system_retain_days = system_retain_days
        self.user_retain_days = user_retain_days
        self.ssh_conn_id = ssh_conn_id
        self.base_log_folder = base_log_folder

    @timing
    def execute(self, context):
        base_log_folder = self.base_log_folder or conf.get('logging', 'base_log_folder')
        system_dag_names = ['dag_processor_manager', 'scheduler']
        try:
            run_date = datetime.strptime(self.run_date, '%Y-%m-%d').date()
        except:
            run_date = datetime.utcnow().date()
        
        dag_names = os.listdir(base_log_folder)
        self.log.info(f"Dag run_date: {run_date}")
        println(f"Clear log inside base_log_folder: {base_log_folder}")
        for dag in dag_names:
            dag_folder = os.path.join(base_log_folder, dag)
            println(f"++ Clear log for sub folder: {dag_folder}")
            if dag not in system_dag_names:
                dag_runs = os.listdir(dag_folder)
                for run_id in dag_runs:
                    scheduled_at = run_id.replace("run_id=scheduled__", "")
                    try:
                        dt = datetime.strptime(scheduled_at, '%Y-%m-%dT%H:%M:%S%z').date()
                    except:
                        try:
                            dt = datetime.strptime(scheduled_at, '%Y-%m-%d').date()
                        except:
                            dt = run_date
                            
                    if dag not in system_dag_names:
                        if dt + timedelta(days=self.user_retain_days) < run_date:
                            clear_fs_obj(os.path.join(dag_folder, run_id))
                    elif dag == 'scheduler':
                        if dt + timedelta(days=self.system_retain_days) < run_date:
                            clear_fs_obj(os.path.join(dag_folder, run_id))
