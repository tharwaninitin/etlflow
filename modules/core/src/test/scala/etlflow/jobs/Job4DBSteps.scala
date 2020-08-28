package etlflow.jobs

import etlflow.EtlJobProps
import etlflow.Schema.EtlJobRun
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps._
import etlflow.spark.SparkUDF
import etlflow.utils.{Config, JDBC}
import zio.Runtime.default.unsafeRun

case class Job4DBSteps(job_properties: EtlJobProps, globalProperties: Config)
  extends GenericEtlJob {

  val delete_credential_script = "delete from credentials where name = 'etlflow'"

  val insert_credential_script = s"""
      insert into credentials values(
      'etlflow',
      'jdbc',
      '{"url" : "${globalProperties.dbLog.url}", "user" : "${globalProperties.dbLog.user}", "password" : "${globalProperties.dbLog.password}", "driver" : "org.postgresql.Driver" }'
      )
      """

  unsafeRun(
    DBQueryStep(
      name  = "DeleteCredential",
      query = delete_credential_script,
      credentials = globalProperties.dbLog
    ).process()
  )

  unsafeRun(
    DBQueryStep(
      name  = "AddCredential",
      query = insert_credential_script,
      credentials = globalProperties.dbLog
    ).process()
  )

  val step1 = DBReadStep[EtlJobRun](
    name  = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun",
    credentials = getCredentials[JDBC]("etlflow")
  )

  def processData(ip: List[EtlJobRun]): Unit = {
    etl_job_logger.info("Processing Data")
    ip.foreach(jr => etl_job_logger.info(jr.toString))
  }

  val step2 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  val job =
    for {
      op2 <- step1.execute()
      _   <- step2.execute(op2)
    } yield ()
}
