# Flink CDC 3.0 QuickStart

> User scenario:
When building their own real-time streaming lakehouse using Doris (Starrocks, Paimon, and Iceberg), users need to continuously synchronize data, including snapshot data and incremental data, from multiple business tables in MySQL database to Doris for creating the ODS layer. 

## Install Flink CDC and required dependencies

Download and extract the flink-cdc-3.0.tar file to a local directory.

Download the required CDC Pipeline Connector JAR from Maven and place it in the lib directory

Configure the `FLINK_HOME` environment variable to load the Flink cluster configuration from the `flink-conf.yaml` file located in the `$FLINK_HOME/conf` directory.

## Write Flink CDC task YAML

```yml
source:
  type: mysql
  host: localhost
  port: 3306
  username: admin
  password: pass
  table: adb.*, bdb.user_table_[0-9]+, [app|web]_order_.*

sink:
  type: doris
  fenodes: FE_IP:HTTP_PORT
  username: admin
  password: pass

pipeline:
  name: mysql-sync-doris
  parallelism: 4
```

## Submit the job to Flink cluster

```bash
# Submit Pipeline
$ ./bin/flink-cdc.sh mysql-to-doris.yaml
Pipeline "mysql-sync-kafka" is submitted with Job ID "DEADBEEF".
```

During the execution of the flink-cdc.sh script, the CDC task configuration is parsed and translated into a DataStream job, which is then submitted to the specified Flink cluster.