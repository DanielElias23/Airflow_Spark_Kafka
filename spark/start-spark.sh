#!/bin/bash
. "/opt/spark/bin/load-spark-env.sh"

if [ "$SPARK_MODE" == "master" ];
then
    export SPARK_MASTER_HOST=`hostname`
    export SPARK_DAEMON_JAVA_OPTS="-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=$JMX_EXPORTER_PORT:/opt/jmx_exporter/config.yaml"

    cd /opt/spark/bin && ./spark-class org.apache.spark.deploy.master.Master \
        --ip $SPARK_MASTER_HOST \
        --port $SPARK_MASTER_PORT \
        --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG
elif [ "$SPARK_MODE" == "worker" ];
then
    export SPARK_DAEMON_JAVA_OPTS="-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=$JMX_EXPORTER_PORT:/opt/jmx_exporter/config.yaml"

    cd /opt/spark/bin && ./spark-class org.apache.spark.deploy.worker.Worker \
        --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG
elif [ "$SPARK_MODE" == "submit" ];
then
    echo "SPARK SUBMIT"
else
    echo "Undefined Workload Type $SPARK_MODE, must specify: master, worker, submit"
fi