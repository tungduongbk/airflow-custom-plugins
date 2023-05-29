
from operators.ms_teams_webhook import MSTeamsWebhookOperator
from datetime import datetime, timedelta


def on_failure(context, **kwargs):
    owner = context['dag'].default_args['owner']
    message = f"""&#x1F4A9; &#x1F4A9; &#x1F4A9; &#x1F4A9; <strong>{owner}</strong>"""
    teams_notification = MSTeamsWebhookOperator(
        status="FAILED",
        task_id="msteams_notify_failure",
        owner=f'{owner}',
        trigger_rule="all_done",
        message=message,
        button_text="View log",
        theme_color="FF0000",
        http_conn_id='ms_team_conn_failure')
    teams_notification.execute(context)


def on_success(context, **kwargs):
    owner = context['dag'].default_args['owner']
    message = f"""A may ding, gut chop &#x1F49E; &#x1F49E; <strong>{owner}</strong>"""
    teams_notification = MSTeamsWebhookOperator(
        status="SUCCESS",
        task_id="msteams_notify_success",
        owner=f'{owner}',
        trigger_rule="all_done",
        message=message,
        button_text="View log",
        theme_color="0072C6",
        http_conn_id='ms_team_conn_success')
    teams_notification.execute(context)


def conditionally_trigger(context, dag_run_obj):
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if c_p:
        consistent = context['params'].get('consistent_run_date', True)  # set default as True
        if consistent:
            run_date = context['dag_run'].conf.get('run_date')
        else:
            run_date = (context['dag_run'].execution_date + timedelta(hours=7)).strftime("%Y-%m-%d")
        dag_run_obj.payload = {'message': context['params']['message'],
                               'run_date': run_date,
                               "dag_controller_id": context['dag_run'].dag_id,
                               "task_controller_id": context['ti'].task_id}
        print(dag_run_obj.payload)
        return dag_run_obj


def receive_trigger_payload(ds, **kwargs):
    """
    Print the payload "message" passed to the DagRun conf attribute.
    :param context: The execution context
    :type context: dict
    """
    print("Received trigger from task {task} in dag {dag} on {run_date}".format(
        dag=kwargs["dag_run"].conf.get('dag_controller_id', ''),
        task=kwargs["dag_run"].conf.get('task_controller_id', ''),
        run_date=kwargs["dag_run"].conf.get("run_date", None)
    ))
