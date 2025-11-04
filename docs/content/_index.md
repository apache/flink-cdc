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

<div class="index-wrapper">
    <div class="top-container bg-purple py-12 block flex xl:items-start justify-between flex-col xl:flex-row">
        <div class="flex flex-col w-full xl:w-[max-content] xl:min-w-[max-content] justify-center text-center xl:text-left">
            <h1 class="header-1 font-bold text-white leading-tight">
                Flink CDC
            </h1>
            <p class="leading-normal text-xl text-white">
                A streaming data integration tool
            </p>
            <div class="text-center xl:text-left">
                <a href="{{< ref "docs/get-started/introduction" >}}" class="mx-auto link-as-button bg-white text-black font-bold px-8">
                    Quick Start
                </a>
            </div>
        </div>
        <div class="w-full text-center mt-8 xl:mt-0 ml-0 xl:ml-16">
            {{< img src="/fig/cdc-flow.png" alt="Flink CDC Flow" >}}
        </div>
    </div>
    <div class="relative -mt-8 bg-purple">
        <svg viewBox="0 0 1440 135" version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
            <g stroke="none" stroke-width="1" fill="#FFFFFF" fill-rule="evenodd">
                <g fill-rule="nonzero">
                    <g id="group-1">
                        <path d="M0,0 C90.7283404,0.913987361 147.912752,26.791023 291.910178,59.0372741 C387.908462,80.534775 543.605069,88.0306277 759,81.5248326 C469.336065,153.973267 216.336065,151.424573 0,73.8787497" id="path-2" opacity="0.202055432"></path>
                        <path d="M100,103.179907 C277.413333,71.1800754 426.147877,51.7578823 546.203633,44.9133275 C666.259389,38.0687728 810.524845,41.1877184 979,54.2701645 C931.069965,55.3032044 810.303266,73.752879 616.699903,109.619188 C423.096539,145.485498 250.863238,143.33907 100,103.179907 Z" id="path-1" opacity="0.100000001"></path>
                    </g>
                    <g id="group-0" transform="translate(0, 35)" fill="#FFFFFF">
                        <path d="M0,33.0606204 C56.6001496,51.914314 97.7011995,64.3217623 123.30215,70.2800135 C180.904789,83.6900143 233.718868,88.4076191 271.437642,91.5254689 C310.739609,94.7722042 395.976162,93.8296671 460.333358,89.7584578 C486.055247,88.1321386 518.005961,84.5528588 556.1855,79.0235702 C594.986722,72.966933 621.598158,68.452987 636.017808,65.4846838 C663.117994,59.9091538 711.68124,48.2789543 726.777545,44.9584296 C779.615613,33.3380687 817.965065,21.7432881 855.430968,15.2222694 C921.762157,3.67668179 954.732352,2.05626571 1010.21307,0 C1059.70784,1.02813285 1096.37415,2.65346824 1120.20801,4.87502229 C1160.65439,8.64517071 1207.54849,17.1083275 1234.31384,21.5189682 C1284.74314,29.8266752 1353.50909,46.2298213 1440,70.7495601 L1440,102.242647 L0,102.242647" id="path-0"></path>
                    </g>
                </g>
            </g>
        </svg>
    </div>
    <div class="bg-white py-8">
        <div class="container mx-auto">
            <h2 class="border-none w-full text-3xl font-bold leading-tight text-center text-primary">
                What is Flink CDC?
            </h2>
            <div class="w-full my-4">
                <div class="divider"></div>
            </div>
            <div class="w-full w-4/5 flex text-center">
                <div class="basis-1/12"></div>
                <p class="text-lg basis-10/12">
                    Flink CDC is a distributed data integration tool for real time data and batch data. Flink CDC brings the simplicity and elegance of data integration via YAML to describe the data movement and transformation.
                </p>
                <div class="basis-1/12"></div>
            </div>
            <div class="w-full flex justify-center	">
                <div class="w-4/5">
                    {{< img src="/fig/index-yaml-example.png" alt="Flink CDC Example" >}}
                </div>
            </div>
        </div>
        <div class="container mx-auto flex flex-wrap mt-6">
            <h2 class="border-none w-full my-2 text-3xl font-bold leading-tight text-center text-primary">
                Key Features
            </h2>
            <div class="w-full my-4">
                <div class="divider"></div>
            </div>
            <div class="w-full md:w-1/3 px-8 py-6 flex flex-col flex-grow flex-shrink">
                <div class="w-full text-lg px-6 text-center text-primary">Change Data Capture</div>
                <div class="w-full my-4">
                    <div class="divider w-1/2 opacity-50"></div>
                </div>
                <p class="text-sm my-0 text-center md:text-left">
                    Flink CDC supports distributed scanning of historical data of database and then automatically switches to change data capturing. The switch uses the incremental snapshot algorithm which ensure the switch action does not lock the database.
                </p>
            </div>
            <div class="w-full md:w-1/3 px-8 py-6 flex flex-col flex-grow flex-shrink">
                <div class="w-full text-lg px-6 text-center text-primary">Schema Evolution</div>
                 <div class="w-full my-4">
                    <div class="divider w-1/2 opacity-50"></div>
                </div>
                <p class="text-sm my-0 text-center md:text-left">
                    Flink CDC has the ability of automatically creating downstream table using the inferred table structure based on upstream table, and applying upstream DDL to downstream systems during change data capturing.
                </p>
            </div>
            <div class="w-full md:w-1/3 px-8 py-6 flex flex-col flex-grow flex-shrink">
                <div class="w-full text-lg px-6 text-center text-primary">Streaming Pipeline</div>
                <div class="w-full my-4">
                    <div class="divider w-1/2 opacity-50"></div>
                </div>
                <p class="text-sm my-0 text-center md:text-left">
                    Flink CDC jobs run in streaming mode by default, providing sub-second end-to-end latency in real-time binlog synchronization scenarios, effectively ensuring data freshness for downstream businesses.
                </p>
            </div>
            <div class="w-full md:w-1/3 px-8 py-6 flex flex-col flex-grow flex-shrink">
                <div class="w-full text-lg px-6 text-center text-primary">Data Transformation</div>
                <div class="w-full my-4">
                    <div class="divider w-1/2 opacity-50"></div>
                </div>
                <p class="text-sm my-0 text-center md:text-left">
                    Flink CDC supports data transform operations of ETL, including column projection, computed column, filter expression and classical scalar functions.
                </p>
            </div>
            <div class="w-full md:w-1/3 px-8 py-6 flex flex-col flex-grow flex-shrink">
                <div class="w-full text-lg px-6 text-center text-primary">Full Database Sync</div>
                 <div class="w-full my-4">
                    <div class="divider w-1/2 opacity-50"></div>
                </div>
                <p class="text-sm my-0 text-center md:text-left">
                    Flink CDC supports synchronizing all tables of source database instance to downstream in one job by configuring the captured database list and table list.
                </p>
            </div>
            <div class="w-full md:w-1/3 px-8 py-6 flex flex-col flex-grow flex-shrink">
                <div class="w-full text-lg px-6 text-center text-primary">Exactly-Once Semantics</div>
                 <div class="w-full my-4">
                    <div class="divider w-1/2 opacity-50"></div>
                </div>
                <p class="text-sm my-0 text-center md:text-left">
                    Flink CDC supports reading database historical data and continues to read CDC events with exactly-once processing, even after job failures.
                </p>
            </div>
        </div>
        <div class="container mx-auto flex flex-wrap mt-6">
            <h2 class="border-none w-full my-2 text-3xl font-bold leading-tight text-center text-primary">
                Learn More
            </h2>
            <div class="w-full my-4">
                <div class="divider"></div>
            </div>
            <div class="w-full md:w-1/2 px-8 py-6 flex flex-col flex-grow flex-shrink">
                <div class="w-full text-lg px-6 text-center text-primary">Try Flink CDC</div>
                 <div class="w-full my-4">
                    <div class="divider w-1/2 opacity-50"></div>
                </div>
                <p class="text-sm my-0 text-center md:text-left">
                    Flink CDC provides a series of quick start demos without any dependencies or java code. A Linux or MacOS computer with Docker installed is enough.
                    Please check out our <a href="{{< ref "docs/get-started/introduction" >}}">Quick Start</a> for more information.
                </p>
            </div>
            <div class="w-full md:w-1/2 px-8 py-6 flex flex-col flex-grow flex-shrink">
                <div class="w-full text-lg px-6 text-center text-primary">Get Help with Flink CDC</div>
                 <div class="w-full my-4">
                    <div class="divider w-1/2 opacity-50"></div>
                </div>
                <p class="text-sm my-0 text-center md:text-left">
                    If you get stuck, check out our <a href="https://flink.apache.org/community.html">community support resources</a>.
                    In particular, Apache Flink’s user mailing list (user@flink.apache.org) is consistently ranked as one of the most active of
                    any Apache project, and is a great way to get help quickly.
                </p>
            </div>
        </div>
    </div>
    <div class="relative bg-purple">
        <svg viewBox="0 0 1440 148" version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
            <g stroke="none" stroke-width="1" fill="#FFFFFF" fill-rule="evenodd">
                <path d="M1440,80.1111111 C1383.555,61.3231481 1342.555,48.925 1317,42.9166667 C1259.5,29.396963 1206.707,24.3442407 1169,20.9814815 C1129.711,17.4775741 1044.426,17.6196759 980,20.9814815 C954.25,22.32525 922.25,25.5039444 884,30.5185185 C845.122,36.0376019 818.455,40.1709537 804,42.9166667 C776.833,48.0762037 728.136,58.9102778 713,61.9907407 C660.023,72.7761759 621.544,83.6674722 584,89.6481481 C517.525,100.238074 484.525,101.510315 429,103 C379.49,101.554185 342.823,99.6467778 319,97.2777778 C278.571,93.2560093 231.737,84.6278519 205,80.1111111 C154.629,71.6002593 86.296,55.069713 0,30.5185185 L0,0 L1440,0 L1440,80.1111111 Z" id="path-0-1" fill="#FFFFFF" fill-rule="nonzero"></path>
                <path d="M338,93.8510923 C530.95466,51.4641029 692.718283,25.7374584 823.290868,16.6711585 C953.863454,7.60485864 1110.7665,11.7362152 1294,29.0652283 C1241.87132,30.4335934 1110.52551,54.8720396 899.962579,102.380567 C689.399649,149.889094 502.078789,147.045936 338,93.8510923 Z" id="path-2" opacity="0.1"></path>
                <path d="M681,12 C771.72834,12.927528 828.912752,39.187927 972.910178,71.9119003 C1068.90846,93.7278826 1224.60507,101.334785 1440,94.7326078 C1150.33606,168.254352 897.336065,165.6679 681,86.9732496" id="path-3" opacity="0.1" transform="translate(1060.5, 80) scale(-1, -1) translate(-1060.5, -80)"></path>
            </g>
        </svg>
    </div>
    <div class="px-6 bg-purple h-40 relative">
        <p class="leading-normal text-lg text-white absolute bottom-0">
            Flink CDC is developed under the umbrella of <a class="text-white" href="https://flink.apache.org">Apache Flink</a>.
        </p>
    </div>
</div>
