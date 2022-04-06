## Without database logging

TASK 1) set below environment variables in your shell.
```shell
 export DB_URL=jdbc:postgresql://localhost:5432/etlflow
 export DB_USER=etlflow
 export DB_PWD=etlflow
 export DB_DRIVER=org.postgresql.Driver
```

TASK 2) To run examples use below commands:
```shell
sbt ";project examplespark; runMain examples.EtlJobParquetToJdbc;"
sbt ";project examplespark; runMain examples.EtlJobParquetToOrc;"
```

TASK 3) To interact with GCP services set below environment variables in your shell
```shell
 export GCS_BUCKET=<..>
 export GOOGLE_APPLICATION_CREDENTIALS=<..>
 export GCP_PROJECT_ID=<..>
```

TASK 4) To run examples use below commands:
```shell
sbt ";project examplespark; runMain examples.EtlJobCsvToCsvGcs;"
sbt ";project examplespark; runMain examples.EtlJobCsvToParquetGcs;"
sbt ";project examplespark; runMain examples.EtlJobBqToJdbc;"
```