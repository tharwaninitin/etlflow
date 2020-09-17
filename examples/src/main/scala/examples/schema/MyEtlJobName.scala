package examples.schema

import etlflow.EtlJobName
import examples.schema.MyEtlJobProps._

sealed trait MyEtlJobName[+EJP] extends EtlJobName[EJP]

object MyEtlJobName {
  val default_ratings_input_path = "data/movies/ratings_parquet/*"
  val default_ratings_intermediate_path = "data/movies/output"
  val default_ratings_input_path_csv = "data/movies/ratings/*"
  val default_output_dataset = "test"
  case object Job0DataprocPARQUETtoORCtoBQ extends MyEtlJobName[EtlJob1Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props(
      ratings_input_path = List(job_properties.getOrElse("ratings_input_path", default_ratings_input_path)),
      ratings_intermediate_path = job_properties.getOrElse("ratings_intermediate_path", default_ratings_intermediate_path),
      ratings_output_dataset = job_properties.getOrElse("ratings_output_dataset", default_output_dataset),
      ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name", "ratings"),
      ratings_output_file_name = Some(job_properties.getOrElse("ratings_output_file_name", "ratings.orc"))
    )
  }
  case object Job1LocalJobDPSparkStep extends MyEtlJobName[SampleProps] {
    override def getActualProperties(job_properties: Map[String, String]): SampleProps = SampleProps()
  }
  case object Job2LocalJobGenericStep extends MyEtlJobName[SampleProps] {
    override def getActualProperties(job_properties: Map[String, String]): SampleProps = SampleProps()
  }
  case object EtlJob2CSVtoPARQUETtoBQLocalWith3Steps extends MyEtlJobName[EtlJob23Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob23Props = EtlJob23Props(
      ratings_input_path = job_properties.getOrElse("ratings_input_path",default_ratings_input_path),
      ratings_output_dataset = job_properties.getOrElse("ratings_output_dataset",default_output_dataset),
      ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name","ratings_par")
    )
  }
  case object EtlJob3CSVtoCSVtoBQGcsWith2Steps extends MyEtlJobName[EtlJob23Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob23Props = EtlJob23Props(
      ratings_input_path = job_properties.getOrElse("ratings_input_path",default_ratings_input_path_csv),
      ratings_output_dataset = job_properties.getOrElse("ratings_output_dataset",default_output_dataset),
      ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name","ratings_par")
    )
  }
  case object EtlJob4BQtoBQ extends MyEtlJobName[EtlJob6Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob6Props = EtlJob6Props()
  }
  case object EtlJob5PARQUETtoJDBC extends MyEtlJobName[EtlJob5Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob5Props = EtlJob5Props(
      ratings_input_path = List(job_properties.getOrElse("ratings_input_path",default_ratings_input_path)),
      ratings_output_table = job_properties.getOrElse("ratings_output_table_name","ratings")
    )
  }
  case object EtlJob6BQPGQuery extends MyEtlJobName[EtlJob4Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props()
  }
}
