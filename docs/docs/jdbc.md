---
layout: docs
title: DBQuery
---

## JDBC Database Steps

**This page shows different JDBC Database Steps available in this library**

### DBQueryStep
We can use below step when we want to trigger query/stored-procedure on JDBC database. Query should not return results for this step 

    val step = DBQueryStep(
        name  = "UpdatePG",
        query = "BEGIN; DELETE FROM ratings WHERE 1 =1; INSERT INTO ratings SELECT * FROM ratings_temp; COMMIT;",
        credentials = JDBC("jdbc_url", "jdbc_user", "jdbc_pwd", "org.postgresql.Driver")
    )

### DBReadStep
We can use below step when we want to trigger query/stored-procedure on JDBC database. Query should  return results for this step 


     case class EtlJobRun(job_name: String,job_run_id: String, state: String)
     
     val step = DBReadStep[EtlJobRun](
        name  = "FetchEtlJobRun",
        query = "SELECT job_name,job_run_id,state FROM jobrun",
        credentials = global_props.dbLog
      )   