package etlflow

import etlflow.schema.Credential.JDBC

trait DbSuiteHelper {
  val credentials: JDBC = JDBC(
    sys.env.getOrElse("LOG_DB_URL","localhost"),
    sys.env.getOrElse("LOG_DB_USER","root"),
    sys.env.getOrElse("LOG_DB_PWD","root"),
    sys.env.getOrElse("LOG_DB_DRIVER","org.postgresql.Driver")
  )
}
