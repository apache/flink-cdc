# Flink CDC 3.0 QuickStart

> 用户场景：
用户基于 Doris(Starrocks、Paimon、Iceberg) 构建自己的实时湖仓，需要将 MySQL 数据库中多个业务表的数据，全增量实时同步至 Doris 用于构建 ODS 层。

## 安装 Flink CDC 及所需依赖

用户下载 flink-cdc-3.0.tar 解压到本地目录，从 Maven 下载需要的 CDC Pipeline Connector JAR 到 lib
配置环境变量 FLINK_HOME，会根据 `$FLINK_HOME/conf/flink-conf.yaml` 读取 Flink 集群配置

## 编写 Flink CDC 任务 YAML

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

## 提交作业至 Flink 集群

```bash
# 提交 Pipeline
$ ./bin/flink-cdc.sh mysql-to-doris.yaml
Pipeline "mysql-sync-kafka" is submitted with Job ID "DEADBEEF".
```

flink-cdc 在执行时，会将 CDC 任务的配置进行解析、翻译为 DataStream 作业、并将作业提交至指定的 Flink 集群。