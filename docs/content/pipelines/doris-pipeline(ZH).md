# Doris Pipeline

本文介绍了Pipeline Doris Connector的使用


## 示例
----------------

```yaml
source:
  type: values
  name: Values Source

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: ""
  table.create.properties.replication_num: 1

pipeline:
  parallelism: 1

```


## Pipeline选项
----------------

<div class="highlight">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width: 10%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 65%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>type</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的Sink, 这里是 <code>'doris'</code>.</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> PipeLine的名称 </td>
    </tr>
     <tr>
      <td>fenodes</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Doris集群FE的Http地址, 比如 127.0.0.1:8030 </td>
    </tr>
     <tr>
      <td>benodes</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Doris集群BE的Http地址, 比如 127.0.0.1:8040 </td>
    </tr>
    <tr>
      <td>jdbc-url</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Doris集群的JDBC地址，比如：jdbc:mysql://127.0.0.1:9030/db</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Doris集群的用户名</td>
    </tr> 
    <tr>
      <td>password</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Doris集群的密码</td>
    </tr>
    <tr>
      <td>auto-redirect</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>String</td>
      <td> 是否通过FE重定向写入，直连BE写入 </td>
    </tr>
    <tr>
      <td>sink.enable.batch-mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td> 是否使用攒批方式写入Doris </td>
    </tr>
    <tr>
      <td>sink.flush.queue-size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">2</td>
      <td>Integer</td>
      <td> 攒批写入的队列大小
      </td>
    </tr>
    <tr>
      <td>sink.buffer-flush.max-rows</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50000</td>
      <td>Integer</td>
      <td>单个批次最大Flush的记录数</td>
    </tr>
    <tr>
      <td>sink.buffer-flush.max-bytes</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10485760(10MB)</td>
      <td>Integer</td>
      <td>单个批次最大Flush的字节数</td>
    </tr>
    <tr>
      <td>sink.buffer-flush.interval</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10s</td>
      <td>String</td>
      <td>Flush的间隔时长，超过这个时间，将异步Flush数据</td>
    </tr>
    <tr>
      <td>sink.properties.</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>StreamLoad的参数。
        For example: <code> sink.properties.strict_mode: true</code>.
        查看更多关于 <a href="https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD/"> StreamLoad的Properties 属性</a></td> 
      </td>
    </tr>
    <tr>
      <td>table.create.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>创建表的Properties配置。
        For example: <code> table.create.properties.replication_num: 1</code>.
        查看更多关于 <a href="https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE/"> Doris Table 的  Properties 属性</a></td> 
      </td>
    </tr>
    </tbody>
</table>
</div>


## 常见问题
--------
* [FAQ(English)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ)
* [FAQ(中文)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH))
