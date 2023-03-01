# No default_args da DAG deve-se passar o parâmetro 'on_failure_callback': alert_failed_task
# Deve-se criar um chat room na GCP e um webhook

import requests
from datetime import timedelta, datetime

def alert_failed_task(context):
    task_id = str(context.get('task_instance').task_id)
    dag_id = str(context.get('task_instance').dag_id)
    execution_date = (context.get('execution_date')-timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S") #fuso horário
    log_url = str(context.get('task_instance').log_url)
    error_message = str(context.get('exception'))
    if len(error_message) > 500:
        error_message = error_message[:200] + \
            '...Error message size is too long, see log below for more details'
    else:
        error_message
    url = 'INSIRA O WEBHOOK DO CHAT ROOM AQUI'
    message = f'''{{
        "text":
        "Task Failed:
            Dag: {dag_id}
            Task: {task_id}
            Execution Date: {execution_date}
            Error: {error_message}
            Log Url: <{log_url}|Clique aqui para ver os logs>"
    }}'''
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    r = requests.post(url, data=message, headers=headers)
    return r
