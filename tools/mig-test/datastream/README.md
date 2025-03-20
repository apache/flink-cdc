# Flink CDC MigrationTestUtils

## DataStream Jobs
### Preparation

1. Install Ruby (macOS has embedded it by default)
2. (Optional) Run `gem install terminal-table` for better display

### Compile DataStream Jobs
3. Go to `tools/mig-test/datastream` and run `ruby compile_jobs.rb` to compile dummy DataStream jobs with specific version tags
4. Then, run `ruby run_migration_test.rb` to start testing