from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator 
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException, AirflowSkipException, AirflowTaskTimeout
from airflow.utils.trigger_rule import TriggerRule
import time
import pandas as pd
import os
#from airflow.operators.oracle_operator import OracleOperator
#OracleOperator.ui_color = '#0000FF'
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Connection
from airflow import settings
from airflow.models import Variable
import logging
from dotenv import load_dotenv
load_dotenv()
import os

def tarea_completada_callback_con_mail(context):
    logging.info("Inicio del callback tarea_completada_callback")
    try:
        from kafka import KafkaProducer
        import resend
        logging.info("Importado KafkaProducer exitosamente")
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        logging.info("KafkaProducer inicializado")
        
        task_instance = context.get('task_instance')
        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        execution_date = context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
        
        message = f"Se completó la tarea {task_id} del DAG {dag_id} en {execution_date}"
        logging.info(f"Mensaje a enviar: {message}")
        
        # Enviar mensaje a Kafka
        producer.send('tareas_completadas', message.encode('utf-8'))
        producer.flush()
        logging.info("Mensaje enviado y flush realizado")
        producer.close()
        

        
        resend.api_key = os.getenv('API_KEY_RESEND')
        email_envio = os.getenv("EMAIL_ENVIO")
        email_destino= os.getenv("EMAIL_DESTINO")
        
        
        r = resend.Emails.send({
             "from": email_envio,
             "to": email_destino,
             "subject": f'Tarea Completada: {task_id} del DAG {dag_id}',
             "html": f"<p>Se completó la tarea {task_id} del DAG {dag_id} en {execution_date}</p>"
             })

        logging.info("Correo electrónico enviado exitosamente")
        
    except Exception as e:
        logging.error(f"Error al enviar el mensaje a Kafka o correo electrónico: {e}", exc_info=True)
    finally:
        logging.info("Fin del callback tarea_completada_callback")

def tarea_completada_callback(context):
    logging.info("Inicio del callback tarea_completada_callback")
    try:
        from kafka import KafkaProducer
        import resend
        logging.info("Importado KafkaProducer exitosamente")
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        logging.info("KafkaProducer inicializado")
        
        task_instance = context.get('task_instance')
        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        execution_date = context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
        
        message = f"Se completó la tarea {task_id} del DAG {dag_id} en {execution_date}"
        logging.info(f"Mensaje a enviar: {message}")
        
        # Enviar mensaje a Kafka
        producer.send('tareas_completadas', message.encode('utf-8'))
        producer.flush()
        logging.info("Mensaje enviado y flush realizado")
        producer.close()

        logging.info("Correo electrónico enviado exitosamente")
        
    except Exception as e:
        logging.error(f"Error al enviar el mensaje a Kafka o correo electrónico: {e}", exc_info=True)
    finally:
        logging.info("Fin del callback tarea_completada_callback")


config = {
    'user': 'root3',
    'password': '1234',
    'host': '172.18.0.8',
    "port": "3306",
    'database': 'mi_base_de_datos'
}

TAGS = ["PythonDataFlow"]
TAG_ID = "Pipeline_Datawarehouse"
DAG_DESCRIPTION = "Extraccion, limpieza, transformación y carga de datos en datawarehouse"
DAG_SCHEDULE = "55 6 * * *"
default_args = {
    "start_date" : datetime(2025, 1, 1),
}
retries = 4
retry_delay = timedelta(minutes=1)

dag = DAG(
    dag_id = TAG_ID,
    description = DAG_DESCRIPTION,
    #Hace que se ponga al día con las ejecuciones que no se han realizado
    catchup = False,
    schedule_interval=DAG_SCHEDULE,
    #maxima cantidad de ejecuciones al mismo tiempo
    max_active_runs = 1,
    default_args = default_args,
    tags = TAGS,
    #dagrun_timeout es el tiempo maximo de ejecucion del dag
    dagrun_timeout = timedelta(minutes=60),
)



##################################################################### INICIO DE DAG ####################################################################

with dag as dag:
        start_customer = EmptyOperator(
        task_id="Customers_Extraction_Source",
        pool="Pool",
        on_success_callback=tarea_completada_callback
        )    
         
        start_orders = EmptyOperator(
        task_id="Orders_Extraction_Source", 
        pool="Pool", 
        on_success_callback=tarea_completada_callback 
        )    
    
        start_product = EmptyOperator(
        task_id="Products_Extraction_Source",
        pool="Pool",   
        on_success_callback=tarea_completada_callback
        )
    
        start_categories = EmptyOperator(
        task_id="Categories_Extraction_Source",
        pool="Pool",   
        on_success_callback=tarea_completada_callback
        )
        
        start_suppliers = EmptyOperator(
        task_id="Suppliers_Extraction_Source",
        pool="Pool",
        on_success_callback=tarea_completada_callback 
        )
    
        start_employees = EmptyOperator(
        task_id="Employees_Extraction_Source",
        pool="Pool",
        on_success_callback=tarea_completada_callback   
        )
        
        start_order_details = EmptyOperator(
        task_id="Order_Details_Extraction_Source",
        pool="Pool",
        on_success_callback=tarea_completada_callback   
        )
        
        start_inventory = EmptyOperator(
        task_id="Inventory_Extraction_Source",
        pool="Pool",
        on_success_callback=tarea_completada_callback
        )

        ####### APLICANDO FUNCIOENS DE EXTRACCION Y LIMPIEZA #######################################
        
        customer_clean = SparkSubmitOperator(
           task_id="Customers_Clean",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Customers_Clean"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5",
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback,
        )
        customer_clean.ui_color = '#c39bd3'
        
        orders_clean = SparkSubmitOperator(
           task_id="Orders_Clean",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Orders_Clean"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        orders_clean.ui_color = '#c39bd3'
        
        products_clean = SparkSubmitOperator(
           task_id="Products_Clean",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Products_Clean"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback,
        )
        products_clean.ui_color = '#c39bd3'
        
        categories_clean = SparkSubmitOperator(
           task_id="Categories_Clean",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Categories_Clean"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback,
        )
        categories_clean.ui_color = '#c39bd3'
        
        suppliers_clean = SparkSubmitOperator(
           task_id="Suppliers_Clean",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Suppliers_Clean"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        suppliers_clean.ui_color = '#c39bd3'
        
        employees_clean = SparkSubmitOperator(
           task_id="Employees_Clean",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Employees_Clean"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        employees_clean.ui_color = '#c39bd3'
        
        order_details_clean = SparkSubmitOperator(
           task_id="Order_Details_Clean",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Order_Details_Clean"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        order_details_clean.ui_color = '#c39bd3'
        
        inventory_clean = SparkSubmitOperator(
           task_id="Inventory_Clean",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Inventory_Clean"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        inventory_clean.ui_color = '#c39bd3'
        

        ################################# FUNIONES DE VALIDACION  ########################################
        
        customer_val = SparkSubmitOperator(
           task_id="Customers_Validation",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Customers_Validation"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        customer_val.ui_color = '#c39bd3'
        
        orders_val = SparkSubmitOperator(
           task_id="Orders_Validation",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Orders_Validation"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        orders_val.ui_color = '#c39bd3'
        
        products_val = SparkSubmitOperator(
           task_id="Products_Validation",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Products_Validation"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        products_val.ui_color = '#c39bd3'
        
        price_product_upt = SparkSubmitOperator(
           task_id="Price_Product_Update",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Price_Product_Update"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        price_product_upt.ui_color = '#c39bd3'
        
        products_upt = SparkSubmitOperator(
           task_id="Products_Update",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Products_Update"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        products_upt.ui_color = '#c39bd3'
        
        categories_val = SparkSubmitOperator(
           task_id="Categories_Validation",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Categories_Validation"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        categories_val.ui_color = '#c39bd3'
        
        suppliers_val = SparkSubmitOperator(
           task_id="Suppliers_Validation",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Suppliers_Validation"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        suppliers_val.ui_color = '#c39bd3'
        
        employees_val = SparkSubmitOperator(
           task_id="Employees_Validation",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Employees_Validation"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        employees_val.ui_color = '#c39bd3'
        
        order_details_val = SparkSubmitOperator(
           task_id="Order_Details_Validation",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Order_Details_Validation"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        order_details_val.ui_color = '#c39bd3'
        
        inventory_val = SparkSubmitOperator(
           task_id="Inventory_Validation",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Inventory_Validation"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        inventory_val.ui_color = '#c39bd3'
        
        ############################### JOIN ##############################################
        
        payment_history = SparkSubmitOperator(
           task_id="Payment_History",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Payment_History"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        payment_history.ui_color = '#f0b27a'
        
        product_catalog = SparkSubmitOperator(
           task_id="Product_Catalog",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Product_Catalog"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        product_catalog.ui_color = '#f0b27a'
        
        order_billing = SparkSubmitOperator(
           task_id="Order_Billing",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Order_Billing"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        order_billing.ui_color = '#f0b27a'
        
        inventory_control = SparkSubmitOperator(
           task_id="Inventory_Control",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["Inventory_Control"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback
        )
        inventory_control.ui_color = '#f0b27a'
        
        
        ##################################################################################
        
        datawarehouse = EmptyOperator(
            task_id="Datawarehouse",
            dag=dag,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            pool="Pool",
            on_success_callback=tarea_completada_callback
        ) 
        
        ###################################################################################
        
        datamart_sales = SparkSubmitOperator(
           task_id="DataMart_Sales",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["DataMart_Sales"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback_con_mail
        )
        datamart_sales.ui_color = '#f7dc6f'
        
        datamart_logistic = SparkSubmitOperator(
           task_id="DataMart_Logistic",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["DataMart_Logistic"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback_con_mail,
        )
        datamart_logistic.ui_color = '#f7dc6f'
        
        datamart_marketing = SparkSubmitOperator(
           task_id="DataMart_Marketing",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["DataMart_Marketing"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback_con_mail
        )
        datamart_marketing.ui_color = '#f7dc6f'
        
        datamart_customer_service = SparkSubmitOperator(
           task_id="DataMart_Customer_Service",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["DataMart_Customer_Service"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback_con_mail
        )
        datamart_customer_service.ui_color = '#f7dc6f'
        
        datamart_human_resource = SparkSubmitOperator(
           task_id="DataMart_Human_Resource",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["DataMart_Human_Resource"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback_con_mail
        )
        datamart_human_resource.ui_color = '#f7dc6f'
        
        datamart_finances = SparkSubmitOperator(
           task_id="DataMart_Finances",
           conn_id="spark_default",
           application="/opt/airflow/dags/include/pyspark_script.py",
           application_args=["DataMart_Finances"],
           jars='/opt/airflow/mysql-connector-j-9.1.0.jar',
           conf={
             "spark.executor.memory": "30g",
             "spark.driver.memory": "30g",
             "spark.executor.cores": "5"
           },
           dag=dag,
           trigger_rule=TriggerRule.ALL_SUCCESS,
           pool="Pool",
           on_success_callback=tarea_completada_callback_con_mail
        )
        datamart_finances.ui_color = '#f7dc6f'
        
        ############ FLUJOS ###################
        
        start_customer >> customer_clean >> customer_val >> payment_history >> datawarehouse
               
        start_orders >> orders_clean >> orders_val >> order_billing >> payment_history >> datawarehouse
    
        start_product >> products_clean >> [products_val, price_product_upt] >> products_upt >> product_catalog >> datawarehouse
    
        start_categories >> categories_clean >> categories_val >> product_catalog >> datawarehouse
        
        start_suppliers >> suppliers_clean >> suppliers_val >> inventory_control >> datawarehouse
        
        start_employees >> employees_clean >> employees_val >> datawarehouse
        
        start_order_details >> order_details_clean >> order_details_val >> order_billing
        
        start_inventory >> inventory_clean >> inventory_val >> inventory_control >> datawarehouse
        
        datawarehouse >> datamart_finances
        
        datawarehouse >> datamart_customer_service
        
        datawarehouse >> datamart_human_resource
        
        datawarehouse >> datamart_logistic
        
        datawarehouse >> datamart_marketing
        
        datawarehouse >> datamart_sales
        