package etlflow

import Schema._
import etlflow.utils.{JDBC, SMTP}

sealed trait MyEtlJobName[+EJP] extends EtlJobName[EJP]

object MyEtlJobName {
  case object EtlJob1 extends MyEtlJobName[EtlJob1Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props()
  }
  case object EtlJob2 extends MyEtlJobName[EtlJob2Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob2Props = EtlJob2Props(
      ratings_output_type = JDBC(
        job_properties("url"),
        job_properties("user"),
        job_properties("pass"),
        "org.postgresql.Driver"
      ),
      smtp_creds = SMTP(
        sys.env.getOrElse("SMTP_PORT","587"),
        sys.env.getOrElse("SMTP_HOST","..."),
        sys.env.getOrElse("SMTP_USER","..."),
        sys.env.getOrElse("SMTP_PASS","..."),
      )
    )
  }
}

