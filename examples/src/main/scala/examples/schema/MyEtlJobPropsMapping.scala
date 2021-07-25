package examples.schema

import etlflow.{EtlJobProps, EtlJobPropsMapping}
import etlflow.etljobs.EtlJob
import etlflow.schema.Executor
import examples.jobs._
import examples.schema.MyEtlJobProps._

sealed trait MyEtlJobPropsMapping[EJP <: EtlJobProps, EJ <: EtlJob[EJP]] extends EtlJobPropsMapping[EJP,EJ]

object MyEtlJobPropsMapping {
  val default_ratings_input_path = "examples/src/main/data/movies/ratings_parquet/ratings.parquet"
  val default_ratings_intermediate_path = "data/movies/output"
  val default_ratings_input_path_csv = "data/movies/ratings/*"
  val default_output_dataset = "test"

  case object DataprocJobPARQUETtoORCtoBQ extends MyEtlJobPropsMapping[EtlJob1Props,EtlJob0DefinitionDataproc] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props(
      ratings_input_path = List(job_properties.getOrElse("ratings_input_path", default_ratings_input_path)),
      ratings_intermediate_path = job_properties.getOrElse("ratings_intermediate_path", default_ratings_intermediate_path),
      ratings_output_dataset = job_properties.getOrElse("ratings_output_dataset", default_output_dataset),
      ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name", "ratings"),
      ratings_output_file_name = Some(job_properties.getOrElse("ratings_output_file_name", "ratings.orc"))
    )
  }
  case object LocalJobDPSpark extends MyEtlJobPropsMapping[SampleProps,EtlJob1DefinitionLocal] {
    override def getActualProperties(job_properties: Map[String, String]): SampleProps = SampleProps()
    override val job_deploy_mode = dataproc
    override val job_schedule = "0 */15 * * * ?"
  }
  case object LocalJobGenericFail extends MyEtlJobPropsMapping[LocalSampleProps,EtlJob2DefinitionLocal] {
    override def getActualProperties(job_properties: Map[String, String]): LocalSampleProps = LocalSampleProps()
    override val job_schedule = "0 */15 * * * ?"
    override val job_max_active_runs = 1
    override val job_retries = 3
    override val job_retry_delay_in_minutes = 1

  }
  val map1 = Map("table_name" -> "ratings_par")
  case object CSVtoPARQUETtoBQLocalWith3Steps extends MyEtlJobPropsMapping[EtlJob23Props,EtlJob2Definition] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob23Props = EtlJob23Props(
      ratings_input_path = job_properties.getOrElse("ratings_input_pat",default_ratings_input_path),
      ratings_output_dataset = job_properties.getOrElse("ratings_output_dataset",default_output_dataset),
      ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name",map1("sa"))
    )

  }
  case object CSVtoCSVtoBQGcsWith2Steps extends MyEtlJobPropsMapping[EtlJob23Props,EtlJob3Definition] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob23Props = EtlJob23Props(
      ratings_input_path = job_properties.getOrElse("ratings_input_path",default_ratings_input_path_csv),
      ratings_output_dataset = job_properties.getOrElse("ratings_output_dataset",default_output_dataset),
      ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name","ratings_par")
    )

  }
  case object BQtoBQ extends MyEtlJobPropsMapping[EtlJob6Props,EtlJob4Definition] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob6Props = EtlJob6Props()
    override val job_deploy_mode = local_subprocess

  }
  case object PARQUETtoJDBC extends MyEtlJobPropsMapping[EtlJob5Props,EtlJob5Definition] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob5Props = EtlJob5Props(
      ratings_input_path = List(job_properties.getOrElse("ratings_input_path",default_ratings_input_path)),
      ratings_output_table = job_properties.getOrElse("ratings_output_table_name","ratings")
    )
    override val job_schedule = "0 0 5,6 ? * *"

  }
  case object BQPGQuery extends MyEtlJobPropsMapping[EtlJob4Props,EtlJob6Definition] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props()
  }
  case object ChildLocal extends MyEtlJobPropsMapping[LocalSampleProps,EtlJob7DefinitionLocal] {
    def getActualProperties(job_properties: Map[String, String]): LocalSampleProps = LocalSampleProps()
    override val job_schedule = "0 */15 * * * ?"
    override val job_max_active_runs = 1
    override val job_retries = 3
    override val job_retry_delay_in_minutes = 1
  }

  case object Job8 extends MyEtlJobPropsMapping[LocalSampleProps,EtlJob8DefinitionLocal] {
    def getActualProperties(job_properties: Map[String, String]): LocalSampleProps = LocalSampleProps()
    override val job_schedule = "0 */15 * * * ?"
    override val job_deploy_mode: Executor = local_subprocess
  }
}
