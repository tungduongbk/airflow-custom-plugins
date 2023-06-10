
from typing import Optional

from paramiko import SFTP_NO_SUCH_FILE

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sensors.base import BaseSensorOperator

class SFTPMultipleFilesSensor(BaseSensorOperator):
    """
    Waits for a file or directory to be present on SFTP.

    :param path: Remote file or directory path
    :type path: str
    :param sftp_conn_id: The connection to run the sensor against
    :type sftp_conn_id: str
    """

    template_fields = ("paths",)

    def __init__(self, *, paths: list, sftp_conn_id: str = "sftp_default", **kwargs) -> None:
        super().__init__(**kwargs)
        self.paths = paths
        self.hook: Optional[SFTPHook] = None
        self.sftp_conn_id = sftp_conn_id

    def poke(self, context: dict) -> bool:
        self.hook = SFTPHook(self.sftp_conn_id)
        self.log.info("Poking for %s", self.paths)
        missed_files = []
        existing_file = []
        for p in self.paths:
            try:
                mod_time = self.hook.get_mod_time(p)
                self.log.info("Found File %s last modified: %s", str(p), str(mod_time))
                existing_file.append(p)
            except OSError as e:
                if e.errno != SFTP_NO_SUCH_FILE:
                    raise e
                missed_files.append(p)
        files_info = {"missing_files": missed_files, "existing_files": existing_file}
        self.xcom_push(context, key="files_info", value=files_info)
        if len(missed_files) > 0:
            self.log.info("Missing files: {}".format(missed_files))
            return False
        self.hook.close_conn()
        return True