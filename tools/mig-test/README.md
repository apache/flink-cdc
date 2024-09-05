# Flink CDC Migration Test Utilities

## Pipeline Jobs
### Preparation

1. Install Ruby (macOS has embedded it by default)
2. (Optional) Run `gem install terminal-table` for better display

### Compile snapshot CDC versions
3. Set `CDC_SOURCE_HOME` to the root directory of the Flink CDC git repository
4. Go to `tools/mig-test` and run `ruby prepare_libs.rb` to download released / compile snapshot CDC versions

### Run migration tests
5. Enter `conf/` and run `docker compose up -d` to start up test containers
6. Set `FLINK_HOME` to the home directory of Flink
7. Go back to `tools/mig-test` and run `ruby run_migration_test.rb` to start testing

### Result
The migration result will be displayed in the console like this:

```
+--------------------------------------------------------------------+
|                       Migration Test Result                        |
+--------------+-------+-------+-------+--------------+--------------+
|              | 3.0.0 | 3.0.1 | 3.1.0 | 3.1-SNAPSHOT | 3.2-SNAPSHOT |
| 3.0.0        | ❓    | ❓    | ❌    | ✅           | ✅           |
| 3.0.1        |       | ❓    | ❌    | ✅           | ✅           |
| 3.1.0        |       |       | ✅    | ❌           | ❌           |
| 3.1-SNAPSHOT |       |       |       | ✅           | ✅           |
| 3.2-SNAPSHOT |       |       |       |              | ✅           |
+--------------+-------+-------+-------+--------------+--------------+
```

> ✅ - Compatible, ❌ - Not compatible, ❓ - Target version doesn't support `--from-savepoint`

## DataStream Jobs

See `datastream/README.md`.
