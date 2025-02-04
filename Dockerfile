# --- Dockerfile para Airflow ---

FROM apache/airflow:latest


USER root
    
# Instalar Java
RUN apt-get update && apt-get install -y openjdk-17-jdk

RUN apt-get update
RUN apt-get install -y curl vim wget software-properties-common ssh net-tools ca-certificates python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas
RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1



ENV SPARK_VERSION=3.5.3 \
HADOOP_VERSION=3 \
SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1

# Instalar Spark
RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz

    
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

COPY mysql-connector-j-9.1.0.jar /opt/airflow/mysql-connector-j-9.1.0.jar
COPY sqlite-jdbc-3.36.0.3.jar /opt/airflow/sqlite-jdbc-3.36.0.3.jar

RUN chown -R airflow:root /opt/airflow /opt/spark /home/airflow
RUN chmod -R u+rwx /opt/airflow /opt/spark /home/airflow
RUN chmod 777 /opt/airflow /opt/spark /home/airflow
RUN echo "umask 002" >> /home/airflow/.bashrc

USER airflow

