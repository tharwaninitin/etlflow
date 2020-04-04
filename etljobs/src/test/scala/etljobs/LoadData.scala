package etljobs

import etljobs.schema.MyEtlJobList._
import etljobs.schema.MyEtlJobProps._
import scala.util.Try

object LoadData extends EtlJobApp[MyEtlJobName[MyEtlJobProps], MyEtlJobProps] {
  // Use AppLogger.initialize() to initialize logging
  // or keep log4j.properties in resources folder
  private val canonical_path = new java.io.File(".").getCanonicalPath
  lazy val global_properties: Option[MyGlobalProperties] = Try(new MyGlobalProperties(s"$canonical_path/etljobs/src/test/resources/loaddata.properties")).toOption
  val etl_job_list_package: String = "etljobs.schema.MyEtlJobList$"

  def toEtlJobProps(job_name: MyEtlJobName[MyEtlJobProps], job_properties: Map[String, String]): MyEtlJobProps = {
    job_name match {
      case EtlJob1PARQUETtoORCtoBQLocalWith2Steps =>
        EtlJob1Props(
          ratings_input_path = List(s"$canonical_path/etljobs/src/test/resources/input/movies/ratings_parquet/*"),
          ratings_output_path = f"gs://${global_properties.get.gcs_output_bucket}/output/ratings",
          ratings_output_dataset = "test",
          ratings_output_table_name = "ratings",
          ratings_output_file_name = Some("ratings.orc")
        )
      case EtlJob2CSVtoPARQUETtoBQLocalWith3Steps =>
        EtlJob23Props(
          job_properties = job_properties,
          ratings_input_path = s"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
          ratings_output_path = f"gs://${global_properties.get.gcs_output_bucket}/output/ratings",
          ratings_output_dataset = "test",
          ratings_output_table_name = "ratings_par"
        )
      case EtlJob3CSVtoCSVtoBQGcsWith2Steps =>
        EtlJob23Props(
          ratings_input_path        = f"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
          ratings_output_path       = f"gs://${global_properties.get.gcs_output_bucket}/output/ratings",
          ratings_output_dataset    = job_properties.getOrElse("ratings_output_dataset","test"),
          ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name","ratings_par")
        )
      case EtlJob4BQtoBQ => EtlJob4Props()
      case EtlJob5PARQUETtoJDBC => EtlJob5Props(
        ratings_input_path = List(s"$canonical_path/etljobs/src/test/resources/input/movies/ratings_parquet/*"),
        ratings_output_table = "ratings",
        jdbc_user = global_properties.get.jdbc_user,
        jdbc_password = global_properties.get.jdbc_pwd,
        jdbc_url = global_properties.get.jdbc_url,
        jdbc_driver = global_properties.get.jdbc_driver
      )
    }
  }
  def toEtlJob(job_name: MyEtlJobName[MyEtlJobProps], job_properties: MyEtlJobProps): EtlJob = {
    job_name match {
      case x@EtlJob1PARQUETtoORCtoBQLocalWith2Steps => etljob1.EtlJobDefinition(x.toString, job_properties, global_properties)
      case x@EtlJob2CSVtoPARQUETtoBQLocalWith3Steps => etljob2.EtlJobDefinition(x.toString, job_properties, global_properties)
      case x@EtlJob3CSVtoCSVtoBQGcsWith2Steps => new etljob3.EtlJobDefinition(x.toString, job_properties, global_properties)
      case x@EtlJob4BQtoBQ => etljob4.EtlJobDefinition(x.toString, job_properties, global_properties)
      case x@EtlJob5PARQUETtoJDBC => etljob5.EtlJobDefinition(x.toString, job_properties, global_properties)
    }
  }
}
