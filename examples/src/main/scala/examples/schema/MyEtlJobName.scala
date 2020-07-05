package examples.schema

import etlflow.EtlJobName
import examples.schema.MyEtlJobProps._

sealed trait MyEtlJobName[+EJP] extends EtlJobName[EJP]

object MyEtlJobName {
  val default_ratings_input_path = "data/movies/ratings_parquet/*"
  val default_ratings_input_path_csv = "data/movies/ratings/*"
  val default_output_dataset = "test"

  case object EtlJob1PARQUETtoORCtoBQLocalWith2Steps extends MyEtlJobName[EtlJob1Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props(
      ratings_input_path = List(job_properties.getOrElse("ratings_input_path",default_ratings_input_path)),
      ratings_output_dataset = job_properties.getOrElse("ratings_output_dataset",default_output_dataset),
      ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name","ratings"),
      ratings_output_file_name = Some("ratings.orc")
    )
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
  case object EtlJob4BQtoBQ extends MyEtlJobName[EtlJob4Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props()
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

  val catOneJobList   = List(EtlJob1PARQUETtoORCtoBQLocalWith2Steps)
  val catTwoJobList   = List(EtlJob2CSVtoPARQUETtoBQLocalWith3Steps,EtlJob3CSVtoCSVtoBQGcsWith2Steps)
  val catThreeJobList = List(EtlJob4BQtoBQ)
  val catFourJobList  = List(EtlJob5PARQUETtoJDBC)
}
