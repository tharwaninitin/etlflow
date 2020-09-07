---
layout: docs
title: DBQuery
---

## JDBC Database Steps

**This page shows different JDBC Database Steps available in this library**

### DBQueryStep
We can use below step when we want to trigger query/stored-procedure on JDBC database. Query should not return results for this step 

```scala mdoc
import etlflow.etlsteps.DBQueryStep
import etlflow.utils.JDBC

val step1 = DBQueryStep(
        name  = "UpdatePG",
        query = "BEGIN; DELETE FROM ratings WHERE 1 =1; INSERT INTO ratings SELECT * FROM ratings_temp; COMMIT;",
        credentials = JDBC("jdbc_url", "jdbc_user", "jdbc_pwd", "org.postgresql.Driver")
)
```

### DBReadStep
We can use below step when we want to trigger query/stored-procedure on JDBC database. Query should  return results for this step 

```scala mdoc

import etlflow.etlsteps.DBReadStep
import etlflow.utils.JDBC
import etlflow.etljobs.GenericEtlJob

case class EtlJobRun(job_name: String,job_run_id: String, state: String)
     
private def step2(cred: JDBC) = DBReadStep[EtlJobRun](
        name  = "FetchEtlJobRun",
        query = "SELECT job_name,job_run_id,state FROM jobrun",
        credentials = cred
)   
```