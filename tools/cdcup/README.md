# cdcup

A `docker` (`compose`) environment on Linux / macOS is required to play with this. Ruby is **not** necessary.

## `./cdcup.sh init`

Initialize a playground environment, and generate configuration files.

## `./cdcup.sh up`

Start docker containers. Note that it may take a while before database is ready.

## `./cdcup.sh pipeline <yaml>`

Submit a YAML pipeline job. Before executing this, please ensure that:

1. All container are running and ready for connections
2. (For MySQL) You've created at least one database & tables to be captured

## `./cdcup.sh flink`

Print Flink Web dashboard URL.

## `./cdcup.sh stop`

Stop all running playground containers.

## `./cdcup.sh down`

Stop and remove containers, networks, and volumes.
