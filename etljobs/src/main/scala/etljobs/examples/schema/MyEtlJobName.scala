package etljobs.examples.schema

import etljobs.{EtlJobName}
import etljobs.examples.schema.MyEtlJobProps._

sealed trait MyEtlJobName[+EJP] extends EtlJobName[EJP]

object MyEtlJobName {
  case object EtlJob1PARQUETtoORCtoBQLocalWith2Steps extends MyEtlJobName[EtlJob1Props] {
    val default_properties: etljobs.EtlJobProps = EtlJob1Props()
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props(
      ratings_input_path = List("data/movies/ratings_parquet/*"),
      ratings_output_dataset = "test",
      ratings_output_table_name = "ratings",
      ratings_output_file_name = Some("ratings.orc")
    )
  }
  case object EtlJob2CSVtoPARQUETtoBQLocalWith3Steps extends MyEtlJobName[EtlJob23Props] {
    val default_properties: etljobs.EtlJobProps = EtlJob23Props(Map.empty)
    def getActualProperties(job_properties: Map[String, String]): EtlJob23Props = EtlJob23Props(
      job_properties = job_properties,
      ratings_input_path = s"data/movies/ratings/*",
      ratings_output_dataset = "test",
      ratings_output_table_name = "ratings_par"
    )
  }
  case object EtlJob3CSVtoCSVtoBQGcsWith2Steps extends MyEtlJobName[EtlJob23Props] {
    val default_properties: etljobs.EtlJobProps = EtlJob23Props(Map.empty)
    def getActualProperties(job_properties: Map[String, String]): EtlJob23Props = EtlJob23Props(
      job_properties = job_properties,
      ratings_input_path = "data/movies/ratings/*",
      ratings_output_dataset = job_properties.getOrElse("ratings_output_dataset","test"),
      ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name","ratings_par")
    )
  }
  case object EtlJob4BQtoBQ extends MyEtlJobName[EtlJob4Props] {
    val default_properties: etljobs.EtlJobProps = EtlJob4Props()
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props()
  }
  case object EtlJob5PARQUETtoJDBC extends MyEtlJobName[EtlJob5Props] {
    val default_properties: etljobs.EtlJobProps = EtlJob4Props()
    def getActualProperties(job_properties: Map[String, String]): EtlJob5Props = EtlJob5Props(
      ratings_input_path = List("data/movies/ratings_parquet/*"),
      ratings_output_table = "ratings"
    )
  }

  val catOneJobList   = List(EtlJob1PARQUETtoORCtoBQLocalWith2Steps)
  val catTwoJobList   = List(EtlJob2CSVtoPARQUETtoBQLocalWith3Steps,EtlJob3CSVtoCSVtoBQGcsWith2Steps)
  val catThreeJobList = List(EtlJob4BQtoBQ)
  val catFourJobList  = List(EtlJob5PARQUETtoJDBC)
}
