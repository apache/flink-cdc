package com.ververica.cdc.debug;

import com.ververica.cdc.cli.CliFrontend;

public class DebugEntry {
    public static void main(String[] args) throws Exception {
        String jobPath = "/Users/lapata/clickzetta_jobs/flink-cdc-3.0/mysqlToDoris.yml";
        String[] extArgs = {jobPath, "--use-mini-cluster", "true"};

        CliFrontend.main(extArgs);
    }
}
