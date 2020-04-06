package etljobs.examples.schema

import etljobs.{EtlJobName}
import etljobs.examples.schema.MyEtlJobProps._

sealed trait MyEtlJobName[+EJP] extends EtlJobName[EJP]

object MyEtlJobName {
  private val canonical_path = new java.io.File(".").getCanonicalPath

  case object EtlJob1PARQUETtoORCtoBQLocalWith2Steps extends MyEtlJobName[EtlJob1Props] {
    val job_props: etljobs.EtlJobProps = EtlJob1Props()
    def toEtlJobProps(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props(
      ratings_input_path = List(s"$canonical_path/data/movies/ratings_parquet/*"),
      ratings_output_dataset = "test",
      ratings_output_table_name = "ratings",
      ratings_output_file_name = Some("ratings.orc")
    )
  }
  case object EtlJob2CSVtoPARQUETtoBQLocalWith3Steps extends MyEtlJobName[EtlJob23Props] {
    val job_props: etljobs.EtlJobProps = EtlJob23Props(Map.empty)
    def toEtlJobProps(job_properties: Map[String, String]): EtlJob23Props = EtlJob23Props(
      job_properties = job_properties,
      ratings_input_path = s"$canonical_path/data/movies/ratings/*",
      ratings_output_dataset = "test",
      ratings_output_table_name = "ratings_par"
    )
  }
  case object EtlJob3CSVtoCSVtoBQGcsWith2Steps extends MyEtlJobName[EtlJob23Props] {
    val job_props: etljobs.EtlJobProps = EtlJob23Props(Map.empty)
    def toEtlJobProps(job_properties: Map[String, String]): EtlJob23Props = EtlJob23Props(
      job_properties = job_properties,
      ratings_input_path = f"$canonical_path/data/movies/ratings/*",
      ratings_output_dataset = job_properties.getOrElse("ratings_output_dataset","test"),
      ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name","ratings_par")
    )
  }
  case object EtlJob4BQtoBQ extends MyEtlJobName[EtlJob4Props] {
    val job_props: etljobs.EtlJobProps = EtlJob4Props()
    def toEtlJobProps(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props()
  }
  case object EtlJob5PARQUETtoJDBC extends MyEtlJobName[EtlJob5Props] {
    val job_props: etljobs.EtlJobProps = EtlJob4Props()
    def toEtlJobProps(job_properties: Map[String, String]): EtlJob5Props = EtlJob5Props(
      ratings_input_path = List(s"$canonical_path/data/movies/ratings_parquet/*"),
      ratings_output_table = "ratings"
    )
  }

  val catOneJobList   = List(EtlJob1PARQUETtoORCtoBQLocalWith2Steps)
  val catTwoJobList   = List(EtlJob2CSVtoPARQUETtoBQLocalWith3Steps,EtlJob3CSVtoCSVtoBQGcsWith2Steps)
  val catThreeJobList = List(EtlJob4BQtoBQ)
  val catFourJobList  = List(EtlJob5PARQUETtoJDBC)
}
