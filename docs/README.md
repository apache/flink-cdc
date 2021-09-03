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
