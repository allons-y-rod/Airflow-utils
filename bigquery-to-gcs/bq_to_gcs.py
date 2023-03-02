import pendulum
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

default_args = {
    'owner': 'RESPONSÁVEL',
    'on_failure_callback': on_failure_callback,
    'on_retry_callback': on_retry_callback,	  
    'start_date' : pendulum.datetime(2022, 3, 1, tzinfo= pendulum.timezone("timezone")), #ex: America/Sao_Paulo
    'depends_on_past': False,
    'retries': 0,
    'instance_name' : datafusion_instance_name,
    'location' : datafusion_location,
    'runtime_args' : {'system.profile.name': datafusion_default_profile_name},
    'namespace' : datafusion_namespace_sales,
    'project_id' : datafusion_project_id,
    'gcp_conn_id' : datafusion_gcp_conn_id
}

with models.DAG(
        'nome_da_dag',
        default_args=default_args,
        max_active_runs = 1,
        catchup=False,
        description = 'Descrição',
        tags=['exemplo','teste'], 
        schedule_interval='cron' #ex: 0 9 * * 1-5
        ) as dag:

    #Criando tabela para exportar arquivos parquet
    execute_create_table = BigQueryInsertJobOperator(
        task_id="execute_create_table",
        gcp_conn_id= bq_gcp_conn_id,
        location= bq_location,
        project_id= project_id,
        configuration={
            "query" : {
                "query" : "{% include 'sql/arquivo_sql_cria_tabela.sql' %}", #arquivo_sql_cria_tabela = arquivo contendo o create or replace a partir de uma consulta sql
                "useLegacySql": False,
            }
        },
    )

    #Gerando Parquet 
    export_tbl = BigQueryToGCSOperator (
        task_id = 'export_tbl',
        gcp_conn_id= bq_gcp_conn_id,
        location= bq_location,
        project_id= project_id,
        source_project_dataset_table = 'dataset.tbl_criada',
        destination_cloud_storage_uris = 'bucket/pasta_destino/arquivo.gz.parquet',
        compression = 'GZIP',
        export_format = 'PARQUET',
        overwrite=True
    )
    execute_create_table >> export_tbl