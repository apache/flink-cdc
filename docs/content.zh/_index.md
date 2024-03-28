---
title: Apache Flink CDC
type: docs
bookToc: false
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<div style="background-image: url('../fig/index-background-header.png'); size:inherit; background-size: auto 100%; overflow: hidden">
    <div style="text-align: center">
        <br><br><br><br><br><br>
        <h1 style="color: #FFFFFF">
            Flink CDC
        </h1>
        <h3 style="color: #FFFFFF">
            A streaming data integration tool
        </h3>
        <br><br><br><br><br><br>
        <br><br><br><br>
    </div>
</div>


<div style="display: flex;">
    <div style="flex: 1;">
    </div>
    <div style="text-align: center; flex: 8;">
          <br><br><br><br>
          {{< img src="/fig/cdc-flow.png" alt="Flink CDC Flow">}}
          <br><br><br><br>
    </div>
    <div style="flex: 1;">
    </div>
</div>


<div style="display: flex;">
    <div style="flex: 1;">
    </div>
    <div style="text-align: center; flex: 8;">
          <p style="color: #BF74F1; font-size: xx-large; padding: 0">What is Flink CDC?</p>
          <hr style="background-color: #BF74F1; width: 60%">
          <br>
          <p style="text-align: center; font-size: large">
            Flink CDC is a distributed data integration tool for real time data and batch data. 
            Flink CDC brings the simplicity and elegance of data integration via YAML to describe
            the data movement and transformation.
          </p>
          <br><br>
          {{< img src="/fig/index-yaml-example.png" alt="Flink CDC Example">}}
          <br><br>
    </div>
    <div style="flex: 1;">
    </div>
    <br><br><br><br> 
</div>


<div style="display: flex;">
    <br><br>
    <div style="flex: 1;">
    </div>
    <div style="text-align: center; flex: 8;">
          <p style="color: #BF74F1; font-size: xx-large; padding: 0">Key Features</p>
    </div>
    <div style="flex: 1;">
    </div>
    <br><br>
</div>

<div style="display: flex;">
    <br><br><br><br>
    <div style="flex: 5%;"></div>
    <div style="text-align: center; flex: 25%;">
          <p style="text-align: center; color: #BF74F1; font-size: large; padding: 0">Change Data Capture</p>
          <hr style="background-color:#BF74F1; width: 60%">
          <p style="text-align: left;">
            Flink CDC supports distributed scanning of historical data of database and then automatically switches to change data capturing. The switch uses the incremental snapshot algorithm which ensure the switch action does not lock the database.  
          </p>
    </div>
    <div style="flex: 8%;"></div>
    <div style="text-align: center; flex: 24%;">
          <p style="text-align: center; color: #BF74F1; font-size: large; padding: 0">Schema Evolution</p>
          <hr style="background-color:#BF74F1; width: 60%">
          <p style="text-align: left;">
            Flink CDC has the ability of automatically creating downstream table using the inferred table structure based on upstream table, and applying upstream DDL to downstream systems during change data capturing.</p>
    </div>
    <div style="flex: 8%;"></div>
    <div style="text-align: center; flex: 25%;">
          <p style="text-align: center; color: #BF74F1; font-size: large; padding: 0">Streaming Pipeline</p>
          <hr style="background-color:#BF74F1; width: 60%">
          <p style="text-align: left;">
            Flink CDC jobs run in streaming mode by default, providing sub-second end-to-end latency in real-time binlog synchronization scenarios, effectively ensuring data freshness for downstream businesses.</p>
    </div>
    <div style="flex: 5%;"></div>
    <br><br><br><br>
    <br><br><br><br>
    <br><br><br><br>
</div>


<div style="display: flex;">
    <br><br><br><br>
    <div style="flex: 5%;"></div>
    <div style="text-align: center; flex: 25%;">
          <p style="text-align: center; color: #BF74F1; font-size: large; padding: 0">Data Transformation</p>
          <hr style="background-color:#BF74F1; width: 60%">
          <p style="text-align: left;">
            Flink CDC will soon support data transform operations of ETL, including column projection, computed column, filter expression and classical scalar functions.</p>
    </div>
    <div style="flex: 8%;"></div>
    <div style="text-align: center; flex: 24%;">
          <p style="text-align: center; color: #BF74F1; font-size: large; padding: 0">Full Database Sync</p>
          <hr style="background-color:#BF74F1; width: 60%">
          <p style="text-align: left;">Flink CDC supports synchronizing all tables of source database instance to downstream in one job by configuring the captured database list and table list.</p>
    </div>
    <div style="flex: 8%;"></div>
    <div style="text-align: center; flex: 25%;">
          <p style="text-align: center; color: #BF74F1; font-size: large; padding: 0">Exactly-Once Semantics</p>
          <hr style="background-color:#BF74F1; width: 60%">
          <p style="text-align: left;">
              Flink CDC supports reading database historical data and continues to read CDC events with exactly-once processing, even after job failures.
          </p>
    </div>
    <div style="flex: 5%;">
    </div>
    <br><br><br><br>
    <br><br><br><br>
    <br><br><br><br>
</div>

<div style="display: flex;">
    <br><br><br><br><br><br>
    <div style="flex: 1;">
    </div>
    <div style="text-align: center; flex: 8;">
          <p style="color: #BF74F1; font-size: xx-large; padding: 0">Learn More</p>
    </div>
    <div style="flex: 1;">
    </div>
    <br><br><br><br>
</div>

<div style="display: flex;">
    <br><br><br><br>
    <div style="flex: 1;">
    </div>
    <div style="text-align: left; flex: 3.5; width: 100%">
          <p style="text-align: left; color: #BF74F1; font-size: x-large; padding: 0">Try Flink CDC</p>
          <p style="text-align: left;">
        Flink CDC provides a series of quick start demos without any dependencies or java code. A Linux or MacOS computer with Docker installed is enough. 
        Please check out our <a href="{{< ref "docs/get-started/introduction" >}}">Quick Start</a> for more information.
         </p>
    </div>
    <div style="flex: 1;"></div>
    <div style="text-align: left; flex: 3.5; width: 100%">
          <p style="text-align: left; color: #BF74F1; font-size: x-large; padding: 0">Get Help with Flink CDC</p>
          <p style="text-align: left;">
            If you get stuck, check out our <a href="https://flink.apache.org/community.html">community support resources</a>. 
                In particular, Apache Flink’s user mailing list (user@flink.apache.org) is consistently ranked as one of the most active of
                any Apache project, and is a great way to get help quickly.</p>
    </div>
    <div style="flex: 1;">
    </div>
    <br><br><br><br>
    <br><br><br><br>
    <br><br><br><br>
</div>

<div style="background-image: url('../fig/index-background-footer.png'); size:inherit; background-size: auto 100%; overflow: hidden">
     <div style="text-align: center">
        <br><br><br><br><br>
        <br><br><br><br><br>
        <h1 style="color: transparent">
             Flink CDC is developed under the umbrella of Apache Flink.
        </h1>
        <p style="color: #FFFFFF; font-size: medium; text-align:left">
             &nbsp;&nbsp;&nbsp;&nbsp;Flink CDC is developed under the umbrella of <a style="color: #FFFFFF" href="https://flink.apache.org">Apache Flink</a>.
        </p>
        <br><br>
    </div>
</div>
