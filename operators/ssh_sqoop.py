from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.sqoop.hooks.sqoop import SqoopHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from hooks.sqoop import SqoopCustomHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from select import select
from airflow.configuration import conf
from base64 import b64encode
from select import select
from typing import Optional, Union


class SSHSqoopImportOperator(BaseOperator):
    template_fields = ('query', 'table', 'warehouse_dir', 'extra_import_options',)
    ui_color = '#33CBFE'

    def __init__(
            self,
            ssh_conn_id='ssh-default',
            sqoop_conn_id='sqoop-default',
            table=None,
            query=None,
            target_dir=None,
            append: bool = False,
            split_by=None,
            file_type=None,
            columns=None,
            where=None,
            direct=None,
            driver=None,
            schema=None,
            ssh_timeout: int = 10,
            keepalive_interval: int = 10,
            extra_import_options=None,
            verbose: bool = False,
            num_mappers: int = None,
            *args, **kwargs):

        super(SSHSqoopImportOperator, self).__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.sqoop_conn_id = sqoop_conn_id
        self.table = table
        self.query = query
        self.target_dir = target_dir
        self.append = append
        self.file_type = file_type
        self.columns = columns
        self.split_by = split_by
        self.where = where
        self.direct = direct
        self.driver = driver
        self.extra_import_options = extra_import_options
        self.verbose = verbose
        self.ssh_timeout = ssh_timeout
        self.sqoop_hook = None
        self.ssh_hook = None
        self.schema = schema
        self.num_mappers = num_mappers
        self.keepalive_interval = keepalive_interval

    def execute(self, context):
        self.sqoop_hook = SqoopCustomHook(schema=self.schema,
                                          num_mappers=self.num_mappers,
                                          conn_id=self.sqoop_conn_id,
                                          verbose=self.verbose)

        if self.query:
            cmd = self.sqoop_hook.import_query_cmd(self.query,
                                                    self.target_dir,
                                                    self.append,
                                                    self.file_type,
                                                    self.split_by,
                                                    self.direct,
                                                    self.driver,
                                                    self.extra_import_options)
        elif self.table:
            cmd = self.sqoop_hook.import_table_cmd(self.table,
                                                    self.target_dir,
                                                    self.append,
                                                    self.file_type,
                                                    self.split_by,
                                                    self.direct,
                                                    self.driver,
                                                    self.extra_import_options)
        else:
            raise AirflowException("Provide query or table parameter to import using Sqoop")

        cmd = " ".join(cmd)

        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    self.log.info(
                        "ssh_hook is not provided or invalid. Trying ssh_conn_id to create SSHHook."
                    )

                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id,
                                            timeout=self.ssh_timeout,
                                            keepalive_interval=self.keepalive_interval)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            with self.ssh_hook.get_conn() as ssh_client:
                self.log.info("Running command: %s", cmd)

                # set timeout taken as params

                stdin, stdout, stderr = ssh_client.exec_command(
                    command=cmd,
                    timeout=self.ssh_timeout
                )
                # get channels
                channel = stdout.channel

                # closing stdin
                stdin.close()
                channel.shutdown_write()

                agg_stdout = b''
                agg_stderr = b''

                # capture any initial output in case channel is closed already
                stdout_buffer_length = len(stdout.channel.in_buffer)

                if stdout_buffer_length > 0:
                    agg_stdout += stdout.channel.recv(stdout_buffer_length)

                # read from both stdout and stderr
                while not channel.closed or channel.recv_ready() or channel.recv_stderr_ready():
                    readq, _, _ = select([channel], [], [], self.ssh_timeout)
                    for recv in readq:
                        if recv.recv_ready():
                            line = stdout.channel.recv(len(recv.in_buffer))
                            agg_stdout += line
                            self.log.info(line.decode('utf-8', 'replace').strip('\n'))
                        if recv.recv_stderr_ready():
                            line = stderr.channel.recv_stderr(len(recv.in_stderr_buffer))
                            agg_stderr += line
                            self.log.warning(line.decode('utf-8', 'replace').strip('\n'))
                    if (
                            stdout.channel.exit_status_ready()
                            and not stderr.channel.recv_stderr_ready()
                            and not stdout.channel.recv_ready()
                    ):
                        stdout.channel.shutdown_read()
                        stdout.channel.close()
                        break

                stdout.close()
                stderr.close()

                exit_status = stdout.channel.recv_exit_status()
                if exit_status == 0:
                    enable_pickling = conf.getboolean('core', 'enable_xcom_pickling')
                    if enable_pickling:
                        return agg_stdout
                    else:
                        return b64encode(agg_stdout).decode('utf-8')

                else:
                    error_msg = agg_stderr.decode('utf-8')
                    raise AirflowException(f"error running cmd: {cmd}, error: {error_msg}")

        except Exception as e:
            raise AirflowException(f"SSH operator error: {str(e)}")
