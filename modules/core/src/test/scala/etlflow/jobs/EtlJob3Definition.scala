package etlflow.jobs

import etlflow.EtlJobProps
import etlflow.Schema.{EtlJob3Props, PostgresData}
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.{DBQueryResultStep, GenericETLStep}
import etlflow.spark.{SparkManager, SparkUDF}
import etlflow.utils.{GlobalProperties, JDBC}

case class EtlJob3Definition(job_properties: EtlJobProps, global_properties: Option[GlobalProperties])
  extends GenericEtlJob with SparkManager with SparkUDF {

  val job_props: EtlJob3Props = job_properties.asInstanceOf[EtlJob3Props]
  private val global_props = global_properties.get

  def processData(OP:List[PostgresData]): Unit = {
    etl_job_logger.info("Processing Data")
    etl_job_logger.info(OP.toString())
  }

  private val step1 = DBQueryResultStep[PostgresData](
    name  = "UpdatePG",
    query = "SELECT job_name,job_run_id,state FROM jobrun  where job_name='EtlJob6BQPGQuery'",
    credentials = JDBC(global_props.log_db_url, global_props.log_db_user, global_props.log_db_pwd, global_props.log_db_driver)
  )

  val step2 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  val job = for {
    result  <-  step1.execute()
       _    <-  step2.execute(result)
  } yield ()
}
