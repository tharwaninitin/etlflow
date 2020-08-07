package etlflow.jobs

import etlflow.Schema.{EtlJob4Props, EtlJobRun, Rating}
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps._
import etlflow.spark.{SparkManager, SparkUDF}
import etlflow.utils.{GlobalProperties, JDBC, PARQUET, UtilityFunctions => UF}
import etlflow.{EtlJobProps, LoggerResource}
import zio.ZIO

case class EtlJob4Definition(job_properties: EtlJobProps, global_properties: Option[GlobalProperties])
  extends GenericEtlJob with SparkManager with SparkUDF {

  private val global_props = global_properties.get
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

  val job: ZIO[LoggerResource, Throwable, Unit] =
    for {
      op2 <- step1.execute()
      _   <- step2.execute(op2)
    } yield ()
}
