package etlflow.jobs

import etlflow.EtlJobProps
import etlflow.Schema.{EtlJob4Props, EtlJobRun}
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps._
import etlflow.spark.SparkUDF
import etlflow.utils.{Config, JDBC}

case class EtlJob4Definition(job_properties: EtlJobProps, globalProperties: Config)
  extends GenericEtlJob  with SparkUDF {

  val job_props: EtlJob4Props = job_properties.asInstanceOf[EtlJob4Props]

  val step1 = DBReadStep[EtlJobRun](
    name  = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun",
    credentials = getCredentials[JDBC]("flyway")
  )

  def processData2(ip: List[EtlJobRun]): Unit = {
    etl_job_logger.info("Processing Data")
    ip.foreach(jr => etl_job_logger.info(jr.toString))
  }

  val step2 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData2,
  )

  val job =
    for {
      op2 <- step1.execute()
      _   <- step2.execute(op2)
    } yield ()
}
