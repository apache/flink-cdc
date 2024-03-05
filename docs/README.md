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

This README gives an overview of how to build the documentation of Flink CDC.

### Build the site locally
Make sure you have installed [Docker](https://docs.docker.com/engine/install/) and started it on you local environment.

From the directory of this module (`docs`), use the following command to start the site.

```sh
./docs_site.sh start
```
Then the site will run and can be viewed at http://localhost:8001, any update on the `docs` will be shown in the site without restarting. 

Of course, you can use the following command to stop the site.

```sh
./docs_site.sh stop
```
