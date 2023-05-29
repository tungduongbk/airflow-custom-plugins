import os
from datetime import timedelta
from typing import Tuple

from airflow.models.baseoperator import BaseOperator
from hooks.amplitude import AmplitudeHook
from utils.parse_date import convert_to_datetime_from_utc, covert_format_date_from_utc, create_path_from_date_time


class AmplitudeDownloadOperator(BaseOperator):
    template_fields = ('op_kwargs',)
    ui_color = '#33BCFE'

    def __init__(self,
                conn_id: str,
                compress: str,
                op_kwargs: dict,
                time_range: int,
                *args,
                **kwargs):
        super(AmplitudeDownloadOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.compress = compress
        self.op_kwargs = op_kwargs
        self.time_range = time_range

    def _download(self,
                api_endpoint,
                compress,
                base_url,
                start,
                end,
                file_path,
                auth: Tuple[str, str]):
        request_uri = """curl -u {login}:{password} '{base}{endpoint}?start={start}&end={end}' >> {file_path}.{type}""".format(
            login=auth[0],
            password=auth[1],
            base=base_url,
            endpoint=api_endpoint,
            start=start,
            end=end,
            type=compress,
            file_path=file_path)

        # print(request_uri)
        self.log.info(" *** Start downloading Amplitude data and put to dir %s.%s" % (file_path, compress))
        os.system(request_uri)
        return

    def execute(self, context):
        run_date = self.op_kwargs.get('run_date')
        amplitude_project_id = self.op_kwargs.get('amplitude_project_id')
        start_datetime = convert_to_datetime_from_utc(run_date)
        end_datetime = start_datetime + timedelta(hours=self.time_range)
        amplitude_hook = AmplitudeHook(amplitude_conn=self.conn_id)
        file_path, file_name = create_path_from_date_time(run_date)
        download_dir = os.path.join(self.op_kwargs.get("local_dir"), amplitude_project_id, file_path)
        print(download_dir)
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        base_url, auth = amplitude_hook.get_conn()
        self._download(api_endpoint=self.op_kwargs.get("api_endpoint"),
                            compress=self.compress,
                            base_url=base_url,
                            auth=auth,
                            start=covert_format_date_from_utc(start_datetime, new_format='%Y%m%dT%H'),
                            end=covert_format_date_from_utc(end_datetime, new_format='%Y%m%dT%H'),
                            file_path=os.path.join(download_dir, file_name))
