# -*- coding: utf-8 -*-
#
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
#
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.models import Variable
from hooks.ms_teams_webhook import MSTeamsWebhookHook
import logging


class MSTeamsWebhookOperator(SimpleHttpOperator):
    """
    This operator allows you to post messages to MS Teams using the Incoming Webhooks connector.
    Takes both MS Teams webhook token directly and connection that has MS Teams webhook token.
    If both supplied, the webhook token will be appended to the host in the connection.

    :param http_conn_id: connection that has MS Teams webhook URL
    :type http_conn_id: str
    :param webhook_token: MS Teams webhook token
    :type webhook_token: str
    :param message: The message you want to send on MS Teams
    :type message: str
    :param title: The subtitle of the message to send
    :type title: str
    :param button_text: The text of the action button
    :type button_text: str
    :param button_url: The URL for the action button click
    :type button_url : str
    :param theme_color: Hex code of the card theme, without the #
    :type message: str
    :param proxy: Proxy to use when making the webhook request
    :type proxy: str
    """

    template_fields = ('message',)

    def __init__(self,
                http_conn_id=None,
                webhook_token=None,
                owner="",
                message="",
                status="",
                button_text="",
                theme_color="00FF00",
                proxy=None,
                *args,
                **kwargs):

        super(MSTeamsWebhookOperator, self).__init__(endpoint=webhook_token, *args, **kwargs)
        self.http_conn_id = http_conn_id
        self.webhook_token = webhook_token
        self.message = message
        self.status = status
        self.owner = owner
        self.button_text = button_text
        self.theme_color = theme_color
        self.proxy = proxy
        self.hook = None

    def execute(self, context):
        dag_id = context['dag_run'].dag_id
        task_id = context['ti'].task_id
        airflow_url = Variable.get("airflow_url")
        logs_url = airflow_url + "/admin/airflow/log?dag_id={}&task_id={}&execution_date={}".format(
            dag_id, task_id, context['ts'])
        self.hook = MSTeamsWebhookHook(
            http_conn_id=self.http_conn_id,
            webhook_token=self.webhook_token,
            message=self.message,
            title=dag_id + "-" + self.status + f"-@{self.owner}",
            button_text=self.button_text,
            button_url=logs_url,
            theme_color=self.theme_color,
            proxy=self.proxy,
            exec_time=context['ts'],
            dag=dag_id,
            task=task_id
        )
        self.hook.execute()
        logging.info("Webhook request sent to MS Teams")
