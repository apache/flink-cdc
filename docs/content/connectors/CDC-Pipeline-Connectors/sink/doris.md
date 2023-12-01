# Doris Pipeline

This article introduces of Doris Pipeline Connector


## Example
----------------

```yaml
source:
   type: values
   name:ValuesSource

sink:
   type: doris
   name: Doris Sink
   fenodes: 127.0.0.1:8030
   username: root
   password: ""
   table.create.properties.replication_num: 1

pipeline:
   pipeline.global.parallelism: 1

```

## Pipeline options
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
       <td>Specify the Sink to use, here is <code>'doris'</code>.</td>
     </tr>
     <tr>
       <td>name</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td> Name of PipeLine </td>
     </tr>
      <tr>
       <td>fenodes</td>
       <td>required</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>Http address of Doris cluster FE, such as 127.0.0.1:8030 </td>
     </tr>
      <tr>
       <td>benodes</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>Http address of Doris cluster BE, such as 127.0.0.1:8040 </td>
     </tr>
     <tr>
       <td>jdbc-url</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>JDBC address of Doris cluster, for example: jdbc:mysql://127.0.0.1:9030/db</td>
     </tr>
     <tr>
       <td>username</td>
       <td>required</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>Username of Doris cluster</td>
     </tr>
     <tr>
       <td>password</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>Password for Doris cluster</td>
     </tr>
     <tr>
       <td>auto-redirect</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">false</td>
       <td>String</td>
       <td> Whether to write through FE redirection and directly connect to BE to write </td>
     </tr>
     <tr>
       <td>sink.enable.batch-mode</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">true</td>
       <td>Boolean</td>
       <td> Whether to use the batch method to write to Doris </td>
     </tr>
     <tr>
           <td>sink.flush.queue-size</td>
           <td>optional</td>
           <td style="word-wrap: break-word;">2</td>
           <td>Integer</td>
           <td> Queue size for batch writing
           </td>
     </tr>
     <tr>
           <td>sink.buffer-flush.max-rows</td>
           <td>optional</td>
           <td style="word-wrap: break-word;">50000</td>
           <td>Integer</td>
           <td>Maximum number of Flush records in a single batch</td>
     </tr>
     <tr>
           <td>sink.buffer-flush.max-bytes</td>
           <td>optional</td>
           <td style="word-wrap: break-word;">10485760(10MB)</td>
           <td>Integer</td>
           <td>Maximum number of bytes flushed in a single batch</td>
     </tr>
     <tr>
       <td>sink.buffer-flush.interval</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">10s</td>
       <td>String</td>
       <td>Flush interval duration. If this time is exceeded, the data will be flushed asynchronously</td>
     </tr>
     <tr>
       <td>sink.properties.</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td> Parameters of StreamLoad.
         For example: <code> sink.properties.strict_mode: true</code>.
         See more about <a href="https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD/"> StreamLoad Properties properties</a></td>
       </td>
     </tr>
     <tr>
       <td>table.create.properties.*</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>Create the Properties configuration of the table.
         For example: <code> table.create.properties.replication_num: 1</code>.
         See more about <a href="https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE/"> Doris Table Properties properties</a></td>
       </td>
     </tr>
     </tbody>
</table>
</div>


## FAQ
--------
* [FAQ(English)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ)
* [FAQ(Chinese)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH))