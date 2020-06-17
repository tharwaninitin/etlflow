---
layout: docs
title: DBQuery
---

## JDBC Database Steps

**This page shows different JDBC Database Steps available in this library**

### DBQueryStep
We can use below step when we want to trigger query/stored-procedure on JDBC database. Query should not return results for this step 

    val step4 = DBQueryStep(
        name  = "UpdatePG",
        query = "BEGIN; DELETE FROM ratings WHERE 1 =1; INSERT INTO ratings SELECT * FROM ratings_temp; COMMIT;",
        credentials = JDBC("jdbc_url", "jdbc_user", "jdbc_pwd", "org.postgresql.Driver")
    )

      