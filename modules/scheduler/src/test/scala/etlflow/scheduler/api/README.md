- To run the Test for EtlFlowServiceTestSuite. Perform below steps : 
  1. Set the `credentials` variable with local postgres connection deatils.
     
     Example : 
     
     ```
     val credentials: JDBC =
      JDBC(
        <local-postgres_url>,
        <local-postgres_user>,
        <local-postgres_pwd>,
        "org.postgresql.Driver"
     )
     ```
  2. Set the below environment variables : 
     ```
     export DP_PROJECT_ID=<...>
     export DP_REGION=<...>
     export DP_ENDPOINT=<...>
     export DP_CLUSTER_NAME=<...>
     export DP_JOB_NAME=SampleJob2LocalJob
     export DP_MAIN_CLASS=examples.LoadData
     export DP_LIBS=gs://<bucket_name>/core/etlflow-core-assembly-x.x.x.jar,gs://<bucket_name>/core/etlflow-examples_2.12-x.x.x.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar,gs://<bucket_name>/core/postgresql-42.2.8.jar
     ```   
