from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import sys
import os
from time import sleep
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.functions import broadcast
from dotenv import load_dotenv
load_dotenv()


url_database=os.getenv('url_database')
database_source=os.getenv('database_source')
user_db_source=os.getenv('user_database_source')
password_db_source=os.getenv('password_database_source')

url_database_mart=os.getenv('url_database_mart')
database_mart=os.getenv('database_mart')
user_db_mart=os.getenv('user_database_mart')
password_db_mart=os.getenv('password_database_mart')

#############################################################################
                          #CARGAS DE DATOS
#############################################################################

def Customers_Clean():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df_mysql = spark.read.format("jdbc") \
        .option("url", f"jdbc:mysql://{url_database}/{database_source}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "ratings") \
        .option("user", f"{user_db_source}") \
        .option("password", f"{password_db_source}") \
        .load()
        
    print(os.getcwd())
    print(os.system("ls"))
    df_mysql.show()
    archivo="/datos_compartidos/Customers_par"

    df_mysql.coalesce(1).write.parquet(archivo, mode='overwrite')

    print("los archivos son: ", os.listdir(archivo))

    spark.stop()

def Orders_Clean():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df_mysql = spark.read.format("jdbc") \
        .option("url", f"jdbc:mysql://{url_database}/{database_source}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "tags") \
        .option("user", f"{user_db_source}") \
        .option("password", f"{password_db_source}") \
        .load()

    df_mysql.show()
    df_mysql.coalesce(1).write.parquet('/datos_compartidos/Order_par', mode='overwrite')

    spark.stop()
    
def Products_Clean():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df_mysql = spark.read.format("jdbc") \
        .option("url", f"jdbc:mysql://{url_database}/{database_source}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "ratings") \
        .option("user", f"{user_db_source}") \
        .option("password", f"{password_db_source}") \
        .load()
   
    df_mysql.show()
    
    df_with_index = df_mysql.withColumn("index", monotonically_increasing_id())

    df1 = df_with_index.select("index", "userId", "movieId")
    df2 = df_with_index.select("index", "rating", "timestamp")
    
    df1.coalesce(1).write.parquet('/datos_compartidos/Products_val_par', mode='overwrite')
    df2.coalesce(1).write.parquet('/datos_compartidos/Products_price_par', mode='overwrite')
    spark.stop()
    
def Categories_Clean():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df_mysql = spark.read.format("jdbc") \
        .option("url", f"jdbc:mysql://{url_database}/{database_source}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "tags") \
        .option("user", f"{user_db_source}") \
        .option("password", f"{password_db_source}") \
        .load()

    df_mysql.show()
    df_mysql.coalesce(1).write.parquet('/datos_compartidos/Categories_par', mode='overwrite')
    spark.stop()

def Suppliers_Clean():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df_mysql = spark.read.format("jdbc") \
        .option("url", f"jdbc:mysql://{url_database}/{database_source}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "tags") \
        .option("user", f"{user_db_source}") \
        .option("password", f"{password_db_source}") \
        .load()

    df_mysql.show()
    df_mysql.coalesce(1).write.parquet('/datos_compartidos/Suppliers_par', mode='overwrite')
    spark.stop()
    
def Employees_Clean():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df_mysql = spark.read.format("jdbc") \
        .option("url", f"jdbc:mysql://{url_database}/{database_source}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "ratings") \
        .option("user", f"{user_db_source}") \
        .option("password", f"{password_db_source}") \
        .load()

    df_mysql.show()
    df_mysql.coalesce(1).write.parquet('/datos_compartidos/Employees_par', mode='overwrite')
    spark.stop()
    
def Order_Details_Clean():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df_mysql = spark.read.format("jdbc") \
        .option("url", f"jdbc:mysql://{url_database}/{database_source}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "tags") \
        .option("user", f"{user_db_source}") \
        .option("password", f"{password_db_source}") \
        .load()

    df_mysql.show()
    df_mysql.coalesce(1).write.parquet('/datos_compartidos/Order_detail_par', mode='overwrite')
    spark.stop()

def Inventory_Clean():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df_mysql = spark.read.format("jdbc") \
        .option("url", f"jdbc:mysql://{url_database}/{database_source}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "ratings") \
        .option("user", f"{user_db_source}") \
        .option("password", f"{password_db_source}") \
        .load()

    df_mysql.show()
    df_mysql.coalesce(1).write.parquet('/datos_compartidos/Inventory_par', mode='overwrite')
    spark.stop()


############################################################################# 
                             #VALIDACION
#############################################################################

def Customers_Validation():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()
            
    df = spark.read.parquet("/datos_compartidos/Customers_par", header=True, inferSchema=True)
    df.coalesce(1).write.parquet('/datos_compartidos/Custormer_val_par', mode='overwrite')
    
    df.show()

    spark.stop()
    
def Orders_Validation():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()
    
    df = spark.read.parquet("/datos_compartidos/Order_par", header=True, inferSchema=True)
    df.coalesce(1).write.parquet('/datos_compartidos/Order_val_par', mode='overwrite')
    df.show()

    spark.stop()
    
def Products_Validation():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df1 = spark.read.parquet("/datos_compartidos/Products_val_par", header=True, inferSchema=True)
    df1.coalesce(1).write.parquet('/datos_compartidos/Products_val_proces_par', mode='overwrite')

    spark.stop()
    
def Categories_Validation():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df = spark.read.parquet("/datos_compartidos/Categories_par", header=True, inferSchema=True)
    df.coalesce(1).write.parquet('/datos_compartidos/Categories_val_par', mode='overwrite')
    df.show()

    spark.stop()
    
def Suppliers_Validation():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df = spark.read.parquet("/datos_compartidos/Suppliers_par", header=True, inferSchema=True)
    df.coalesce(1).write.parquet('/datos_compartidos/Suppliers_var_par', mode='overwrite')

    df.show()

    spark.stop()
    
def Employees_Validation():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df = spark.read.parquet("/datos_compartidos/Employees_par", header=True, inferSchema=True)
    df.coalesce(1).write.parquet('/datos_compartidos/datawarehouse/Employees_var_par', mode='overwrite')

    df.show()

    spark.stop()
    
def Price_Product_Update():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df = spark.read.parquet("/datos_compartidos/Products_price_par", header=True, inferSchema=True)
    df.coalesce(1).write.parquet('/datos_compartidos/Products_price_proces_par', mode='overwrite')

    df.show()

    spark.stop()
    
def Products_Update():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df1 = spark.read.parquet("/datos_compartidos/Products_price_proces_par", header=True, inferSchema=True)
    df2 = spark.read.parquet("/datos_compartidos/Products_val_proces_par", header=True, inferSchema=True)
    
    df1.show()
    df2.show()
    
    df_merged = df1.join(df2, on="index", how="inner")
    df_merged = df_merged.drop("index")
    
    df_merged.coalesce(1).write.parquet('/datos_compartidos/Products_upt_par', mode='overwrite')

    df_merged.show()

    spark.stop()

def Order_Details_Validation():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df = spark.read.parquet("/datos_compartidos/Order_detail_par", header=True, inferSchema=True)
    df.coalesce(1).write.parquet('/datos_compartidos/Order_detail_val_par', mode='overwrite')

    df.show()

    spark.stop()
    
def Inventory_Validation():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df = spark.read.parquet("/datos_compartidos/Inventory_par", header=True, inferSchema=True)
    df.coalesce(1).write.parquet('/datos_compartidos/Inventory_val_par', mode='overwrite')

    df.show()

    spark.stop()
    
##############################################################################

def Payment_History():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df = spark.read.parquet("/datos_compartidos/Order_billing_par", header=True, inferSchema=True)
    df2 = spark.read.parquet("/datos_compartidos/Custormer_val_par", header=True, inferSchema=True)
    df.show()
    print(df.count())
    df2.show()
    print(df2.count())
    df2 = df2.drop("movieId")
    df2 = df2.drop("timestamp")
    df_join = df.join(df2, "userId", "left")
    df_join = df_join.limit(32000000)
    print(df_join.count())
    df_join.coalesce(1).write.parquet('/datos_compartidos/datawarehouse/Payment_history_par', mode='overwrite')
    df_join.show()

    spark.stop()
    
def Product_Catalog():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df = spark.read.parquet("/datos_compartidos/Categories_val_par", header=True, inferSchema=True)
    df2 = spark.read.parquet("/datos_compartidos/Products_upt_par", header=True, inferSchema=True)
    df.show()
    df2.show()
    df2 = df2.drop("movieId")
    df2 = df2.drop("timestamp")
    df_join = df.join(broadcast(df2), "userId", "left")
    df_join = df_join.limit(32000000)
    print(df_join.count())
    df_join.coalesce(1).write.parquet('/datos_compartidos/datawarehouse/Products_catalog_par', mode='overwrite')
    df_join.show()

    spark.stop()
    
def Order_Billing():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df = spark.read.parquet("/datos_compartidos/Order_val_par", header=True, inferSchema=True)
    df2 = spark.read.parquet("/datos_compartidos/Order_detail_val_par", header=True, inferSchema=True)
    df.show()
    df2.show()
    df = df.drop("userId")
    df = df.drop("tag")
    df2 = df2.drop("timestamp")
    df_join = df2.join(broadcast(df), "movieId", "left")
    df_join = df_join.limit(2000000)
    print(df_join.count())
    df_join.coalesce(1).write.parquet('/datos_compartidos/Order_billing_par', mode='overwrite')
    df_join.show()

    spark.stop()
    
def Inventory_Control():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df = spark.read.parquet("/datos_compartidos/Inventory_val_par", header=True, inferSchema=True)
    df2 = spark.read.parquet("/datos_compartidos/Suppliers_var_par", header=True, inferSchema=True)
    df.show()
    df2.show()
    df.count()
    df2.count()
    df2 = df2.drop("movieId")
    df2 = df2.drop("timestamp")
    df_join = df.join(broadcast(df2), "userId", "left")    
    df_join = df_join.limit(32000000)
    print(df_join.count())
    df_join.coalesce(1).write.parquet('/datos_compartidos/datawarehouse/Inventory_control_par', mode='overwrite')
    df_join.show()

    spark.stop()

##############################################################################

def DataMart_Sales():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()

    df = spark.read.parquet("/datos_compartidos/datawarehouse/Inventory_control_par", header=True, inferSchema=True)
    
    jdbc_url = f"jdbc:mysql://{url_database_mart}/{database_mart}"
    connection_properties = {
        "user": f"{user_db_mart}",
        "password": f"{password_db_mart}",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    df.printSchema()
    
    df = df.withColumn("partition_id", monotonically_increasing_id())
    partition_column = "partition_id"
    bounds = df.selectExpr(f"min({partition_column}) as min_value", f"max({partition_column}) as max_value").collect()[0]
    lower_bound = bounds['min_value']
    upper_bound = bounds['max_value']
    print(f"lower_bound: {lower_bound}, upper_bound: {upper_bound}")
    num_partitions = 20
    batch_size = 20000
        
    try:
        df.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", "Sales") \
          .option("user", user_db_mart) \
          .option("password", password_db_mart) \
          .option("driver", "com.mysql.cj.jdbc.Driver") \
          .option("numPartitions", num_partitions) \
          .option("batchsize", batch_size) \
          .option("partitionColumn", partition_column) \
          .option("lowerBound", str(lower_bound)) \
          .option("upperBound", str(upper_bound)) \
          .mode('overwrite') \
          .save()
    except Exception as e:
        print("Error al escribir data:", e)
    
    spark.stop()
    sleep(10)
    
def DataMart_Logistic():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()
    
    df2 = spark.read.parquet("/datos_compartidos/datawarehouse/Inventory_control_par", header=True, inferSchema=True)
    
    jdbc_url = f"jdbc:mysql://{url_database_mart}/{database_mart}"
    connection_properties = {
        "user": f"{user_db_mart}",
        "password": f"{password_db_mart}",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    df2.printSchema()
    
    df2 = df2.withColumn("partition_id", monotonically_increasing_id())
    partition_column = "partition_id"
    bounds = df2.selectExpr(f"min({partition_column}) as min_value", f"max({partition_column}) as max_value").collect()[0]
    lower_bound = bounds['min_value']
    upper_bound = bounds['max_value']
    print(f"lower_bound: {lower_bound}, upper_bound: {upper_bound}")
    num_partitions = 20
    batch_size = 20000
        
    try:
        df2.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", "Logistic") \
          .option("user", user_db_mart) \
          .option("password", password_db_mart) \
          .option("driver", "com.mysql.cj.jdbc.Driver") \
          .option("numPartitions", num_partitions) \
          .option("batchsize", batch_size) \
          .option("partitionColumn", partition_column) \
          .option("lowerBound", str(lower_bound)) \
          .option("upperBound", str(upper_bound)) \
          .mode('overwrite') \
          .save()

    except Exception as e:
        print("Error al escribir data:", e)
    
    spark.stop()
    sleep(10)
    
def DataMart_Marketing():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()
    
    df3 = spark.read.parquet("/datos_compartidos/datawarehouse/Inventory_control_par", header=True, inferSchema=True)
    
    jdbc_url = f"jdbc:mysql://{url_database_mart}/{database_mart}"
    connection_properties = {
        "user": f"{user_db_mart}",
        "password": f"{password_db_mart}",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    df3.printSchema()
    
    df3 = df3.withColumn("partition_id", monotonically_increasing_id())
    partition_column = "partition_id"
    bounds = df3.selectExpr(f"min({partition_column}) as min_value", f"max({partition_column}) as max_value").collect()[0]
    lower_bound = bounds['min_value']
    upper_bound = bounds['max_value']
    print(f"lower_bound: {lower_bound}, upper_bound: {upper_bound}")
    num_partitions = 20
    batch_size = 20000
        
    try:
        df3.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", "Marketing") \
          .option("user", user_db_mart) \
          .option("password", password_db_mart) \
          .option("driver", "com.mysql.cj.jdbc.Driver") \
          .option("numPartitions", num_partitions) \
          .option("batchsize", batch_size) \
          .option("partitionColumn", partition_column) \
          .option("lowerBound", str(lower_bound)) \
          .option("upperBound", str(upper_bound)) \
          .mode('overwrite') \
          .save()
      
        print("DataFrame 'df3' escrito en la tabla 'securities_final'.")
    except Exception as e:
        print("Error al escribir data:", e)
    spark.stop()
    sleep(10)
    
    
def DataMart_Customer_Service():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()    
    
    df4 = spark.read.parquet("/datos_compartidos/datawarehouse/Inventory_control_par", header=True, inferSchema=True)
    
    jdbc_url = f"jdbc:mysql://{url_database_mart}/{database_mart}"
    connection_properties = {
        "user": f"{user_db_mart}",
        "password": f"{password_db_mart}",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    df4.printSchema()
    df4 = df4.withColumn("partition_id", monotonically_increasing_id())
    partition_column = "partition_id"
    bounds = df4.selectExpr(f"min({partition_column}) as min_value", f"max({partition_column}) as max_value").collect()[0]
    lower_bound = bounds['min_value']
    upper_bound = bounds['max_value']
    print(f"lower_bound: {lower_bound}, upper_bound: {upper_bound}")
    num_partitions = 20
    batch_size = 20000
        
    try:
        df4.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", "Customer_Service") \
          .option("user", user_db_mart) \
          .option("password", password_db_mart) \
          .option("driver", "com.mysql.cj.jdbc.Driver") \
          .option("numPartitions", num_partitions) \
          .option("batchsize", batch_size) \
          .option("partitionColumn", partition_column) \
          .option("lowerBound", str(lower_bound)) \
          .option("upperBound", str(upper_bound)) \
          .mode('overwrite') \
          .save()
          
    except Exception as e:
        print("Error al escribir data", e)
    
    spark.stop()
    sleep(10)
    
    
def DataMart_Human_Resource():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()    
    
    df4 = spark.read.parquet("/datos_compartidos/datawarehouse/Inventory_control_par", header=True, inferSchema=True)
    
    jdbc_url = f"jdbc:mysql://{url_database_mart}/{database_mart}"
    connection_properties = {
        "user": f"{user_db_mart}",
        "password": f"{password_db_mart}",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    df4.printSchema()
    df4 = df4.withColumn("partition_id", monotonically_increasing_id())
    partition_column = "partition_id"
    bounds = df4.selectExpr(f"min({partition_column}) as min_value", f"max({partition_column}) as max_value").collect()[0]
    lower_bound = bounds['min_value']
    upper_bound = bounds['max_value']
    print(f"lower_bound: {lower_bound}, upper_bound: {upper_bound}")
    num_partitions = 20
    batch_size = 20000
        
    try:
        df4.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", "Logistic") \
          .option("user", user_db_mart) \
          .option("password", password_db_mart) \
          .option("driver", "com.mysql.cj.jdbc.Driver") \
          .option("numPartitions", num_partitions) \
          .option("batchsize", batch_size) \
          .option("partitionColumn", partition_column) \
          .option("lowerBound", str(lower_bound)) \
          .option("upperBound", str(upper_bound)) \
          .mode('overwrite') \
          .save()

    except Exception as e:
        print("Error al escribir data", e)
        
    spark.stop()
    sleep(10)
    
     
def DataMart_Finances():
    spark = SparkSession.builder \
        .appName("My Spark App") \
        .config("spark.hadoop.fs.permissions.umask-mode", '000') \
        .getOrCreate()    
    
    df4 = spark.read.parquet("/datos_compartidos/datawarehouse/Inventory_control_par", header=True, inferSchema=True)
    
    jdbc_url = f"jdbc:mysql://{url_database_mart}/{database_mart}"
    connection_properties = {
        "user": f"{user_db_mart}",
        "password": f"{password_db_mart}",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    df4.printSchema()
    df4 = df4.withColumn("partition_id", monotonically_increasing_id())
    partition_column = "partition_id"
    bounds = df4.selectExpr(f"min({partition_column}) as min_value", f"max({partition_column}) as max_value").collect()[0]
    lower_bound = bounds['min_value']
    upper_bound = bounds['max_value']
    print(f"lower_bound: {lower_bound}, upper_bound: {upper_bound}")
    num_partitions = 20
    batch_size = 20000
        
    try:
        df4.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", "Finances") \
          .option("user", user_db_mart) \
          .option("password", password_db_mart) \
          .option("driver", "com.mysql.cj.jdbc.Driver") \
          .option("numPartitions", num_partitions) \
          .option("batchsize", batch_size) \
          .option("partitionColumn", partition_column) \
          .option("lowerBound", str(lower_bound)) \
          .option("upperBound", str(upper_bound)) \
          .mode('overwrite') \
          .save()

    except Exception as e:
        print("Error al escribir data", e)
    
    spark.stop()
    sleep(10)
    
##############################################################################
                     #EJECUCIÓN DE LAS TAREAS    
##############################################################################

if __name__ == "__main__":

    if len(sys.argv) > 1:
        funcion_a_ejecutar = sys.argv[1]
        if funcion_a_ejecutar == "Customers_Clean":
            Customers_Clean()
        elif funcion_a_ejecutar == "Orders_Clean":
            Orders_Clean()
        elif funcion_a_ejecutar == "Products_Clean":
            Products_Clean()
        elif funcion_a_ejecutar == "Categories_Clean":
            Categories_Clean()
        elif funcion_a_ejecutar == "Suppliers_Clean":
            Suppliers_Clean()
        elif funcion_a_ejecutar == "Employees_Clean":
            Employees_Clean()
        elif funcion_a_ejecutar == "Order_Details_Clean":
            Order_Details_Clean()
        elif funcion_a_ejecutar == "Inventory_Clean":
            Inventory_Clean()
            
        elif funcion_a_ejecutar == "Customers_Validation":
            Customers_Validation()
        elif funcion_a_ejecutar == "Orders_Validation":
            Orders_Validation()
        elif funcion_a_ejecutar == "Products_Validation":
            Products_Validation()
        elif funcion_a_ejecutar == "Price_Product_Update":
            Price_Product_Update()
        elif funcion_a_ejecutar == "Products_Update":
            Products_Update()
        elif funcion_a_ejecutar == "Categories_Validation":
            Categories_Validation()
        elif funcion_a_ejecutar == "Suppliers_Validation":
            Suppliers_Validation()
        elif funcion_a_ejecutar == "Employees_Validation":
            Employees_Validation()
        elif funcion_a_ejecutar == "Order_Details_Validation":
            Order_Details_Validation()
        elif funcion_a_ejecutar == "Inventory_Validation":
            Inventory_Validation()
            
        elif funcion_a_ejecutar == "Payment_History":
            Payment_History()
        elif funcion_a_ejecutar == "Product_Catalog":
            Product_Catalog()
        elif funcion_a_ejecutar == "Order_Billing":
            Order_Billing()
        elif funcion_a_ejecutar == "Inventory_Control":
            Inventory_Control()
        
        elif funcion_a_ejecutar == "DataMart_Sales":
            DataMart_Sales()
        elif funcion_a_ejecutar == "DataMart_Logistic":
            DataMart_Logistic()
        elif funcion_a_ejecutar == "DataMart_Marketing":
            DataMart_Marketing()
        elif funcion_a_ejecutar == "DataMart_Customer_Service":
            DataMart_Customer_Service()
        elif funcion_a_ejecutar == "DataMart_Human_Resource":
            DataMart_Human_Resource()
        elif funcion_a_ejecutar == "DataMart_Finances":
            DataMart_Finances()
        
        else:
            print(f"Función desconocida: {funcion_a_ejecutar}")
            sys.exit(1)
    else:
        print("Por favor, proporciona el nombre de la función a ejecutar.")
        sys.exit(1)
