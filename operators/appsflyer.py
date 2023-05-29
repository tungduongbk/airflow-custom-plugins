from airflow.models import BaseOperator
from api.appsflyer.report_api import AppsflyerReportApi
from utils.common import timing


class AppsflyerDownloadOperator(BaseOperator):
    template_fields = ("run_date", "download_dir")

    def __init__(self,
                run_date: str,
                download_dir: str,
                app_id: str = None,
                report_type: str = None,
                **kwargs):
        super(AppsflyerDownloadOperator, self).__init__(**kwargs)
        self.run_date = run_date
        self.download_dir = download_dir
        self.app_id = app_id
        self.report_type = report_type

    @timing
    def execute(self, context):
        api = AppsflyerReportApi()
        api.download_report_type(self.run_date, 
            app_id=self.app_id,
            report_type=self.report_type,
            download_dir=self.download_dir)
