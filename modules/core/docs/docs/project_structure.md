---
layout: docs
title: Project Structure
---

## Project Structure

**EtlFlow project contains following modules:**

1. **modules/core**:
 This module contains core library which defines Scala internal **dsl** that assists with writing **ETL Job** which can be composed as multiple **ETL steps** in a concise manner which facilitates easier **Testing** and reasoning about the entire job. 
 This module also contains many [test jobs](https://github.com/tharwaninitin/etlflow/blob/master/modules/core/src/test/scala) which contains multiple steps. This core library also contains tests and all jobs uses EtlJob API. 
 To run all test successfully some properties needs to be set in [loaddata.properties](https://github.com/tharwaninitin/etlflow/blob/master/modules/core/src/test/resources/loaddata.properties) or set these properties as ENVIRONMENT variables.
 ```shell
 export GCS_OUTPUT_BUCKET=<...>
 export GCP_PROJECT=<...>
 export GOOGLE_APPLICATION_CREDENTIALS=<...> # this should be full path to Service Account Key Json which should have GCS and Biguery Read/Write access
 ```
 Also below env variables for logging in PG database which needs to be set before running
 ```shell
  export LOG_DB_URL=<...>
  export LOG_DB_USER=<...>
  export LOG_DB_PWD=<...> 
  ```
 Now run tests using below sbt command
 ```shell
 sbt "project etlflow" test
 ```

2. **modules/core/src/main/scala/etlflow/etlsteps**:
 This package contains all type of ETL Steps that can be created with this library.

3. **modules/examples**:
 This module provides examples of different types of ETL Jobs which can be created with this library.
