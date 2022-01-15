package examples.schema

import etlflow.{EtlJobProps, EtlJobPropsMapping}
import etlflow.etljobs.EtlJob
import examples.jobs._
import examples.schema.MyEtlJobProps._
import io.circe.generic.auto._

sealed trait MyEtlJobPropsMapping[EJP <: EtlJobProps, EJ <: EtlJob[EJP]] extends EtlJobPropsMapping[EJP, EJ]

object MyEtlJobPropsMapping {
  val default_ratings_input_path        = "exampleserver/src/main/data/movies/ratings_parquet/ratings.parquet"
  val default_ratings_intermediate_path = "exampleserver/src/main/data/movies/output"
  val default_ratings_input_path_csv    = "exampleserver/src/main/data/movies/ratings/*"
  val default_output_dataset            = "dev"

  case object EtlJobBqLoadStep extends MyEtlJobPropsMapping[EtlJob1Props, EtlJobBqLoadStep] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props(
      ratings_input_path = job_properties.getOrElse("ratings_input_path", sys.env("GCS_INPUT_LOCATION")),
      ratings_output_dataset = job_properties.getOrElse("ratings_output_dataset", default_output_dataset),
      ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name", "ratings"),
      ratings_output_file_name = Some(job_properties.getOrElse("ratings_output_file_name", "ratings.parquet"))
    )
  }
  case object EtlJobDpSparkJobStep extends MyEtlJobPropsMapping[SampleProps, EtlJobDpSparkJobStep] {
    override def getActualProperties(job_properties: Map[String, String]): SampleProps = SampleProps()

    override val job_deploy_mode = dataproc
    override val job_schedule    = "0 */15 * * * ?"
  }
  case object EtlJobGenericStep extends MyEtlJobPropsMapping[LocalSampleProps, EtlJobGenericStep] {
    override def getActualProperties(job_properties: Map[String, String]): LocalSampleProps = LocalSampleProps()

    override val job_schedule               = "0 */15 * * * ?"
    override val job_max_active_runs        = 1
    override val job_retries                = 3
    override val job_retry_delay_in_minutes = 1

  }
  case object EtlJobBqLoadStepWithSchema extends MyEtlJobPropsMapping[EtlJob2Props, EtlJobBqLoadStepWithSchema] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob2Props = EtlJob2Props(
      ratings_input_path = job_properties.getOrElse("ratings_input_path", sys.env("GCS_INPUT_LOCATION")),
      ratings_output_dataset = job_properties.getOrElse("ratings_output_dataset", default_output_dataset),
      ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name", "ratings_par"),
      ratings_output_file_name = Some(job_properties.getOrElse("ratings_output_file_name", "ratings.csv"))
    )

  }
  case object EtlJobBqLoadStepWithQuery extends MyEtlJobPropsMapping[EtlJob6Props, EtlJobBqLoadStepWithQuery] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob6Props = EtlJob6Props()
    override val job_deploy_mode                                               = local_subprocess

  }
  case object EtlJobDbQueryStep extends MyEtlJobPropsMapping[EtlJob4Props, EtlJobDbQueryStep] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props()
  }
  case object EtlJobEtlFlowJobStep extends MyEtlJobPropsMapping[LocalSampleProps, EtlJobEtlFlowJobStep] {
    def getActualProperties(job_properties: Map[String, String]): LocalSampleProps = LocalSampleProps()

    override val job_schedule               = "0 */15 * * * ?"
    override val job_max_active_runs        = 1
    override val job_retries                = 3
    override val job_retry_delay_in_minutes = 1
  }
}
