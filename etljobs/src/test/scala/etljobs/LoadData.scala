package etljobs

import etljobs.schema.EtlJobList
import etljobs.schema.EtlJobList.{EtlJob1PARQUETtoORCtoBQLocalWith2Steps, EtlJob2CSVtoPARQUETtoBQLocalWith3Steps, EtlJob3CSVtoCSVtoBQGcsWith2Steps, MyEtlJobName}
import etljobs.schema.EtlJobProps.{EtlJob1Props, EtlJob23Props, MyEtlJobProps}
import etljobs.utils.{UtilityFunctions => UF}
import scala.util.Try

object LoadData extends EtlJobApp[MyEtlJobName, MyEtlJobProps] {
  private val canonical_path = new java.io.File(".").getCanonicalPath
  val global_properties: Option[MyGlobalProperties] = Try(new MyGlobalProperties(s"$canonical_path/etljobs/src/test/resources/loaddata.properties")).toOption

  def toEtlJobPropsAsJson(job_name: MyEtlJobName): Map[String,String] = {
    job_name match {
      case EtlJobList.EtlJob1PARQUETtoORCtoBQLocalWith2Steps => UF.getEtlJobProps[EtlJob1Props]()
      case EtlJobList.EtlJob2CSVtoPARQUETtoBQLocalWith3Steps => UF.getEtlJobProps[EtlJob23Props]()
      case EtlJobList.EtlJob3CSVtoCSVtoBQGcsWith2Steps => UF.getEtlJobProps[EtlJob23Props]()
    }
  }
  def toEtlJobProps(job_name: MyEtlJobName, job_properties: Map[String, String]): MyEtlJobProps = {
    job_name match {
      case EtlJob1PARQUETtoORCtoBQLocalWith2Steps =>
        EtlJob1Props(
          job_name = EtlJob1PARQUETtoORCtoBQLocalWith2Steps,
          ratings_input_path = List(s"$canonical_path/etljobs/src/test/resources/input/movies/ratings_parquet/*"),
          ratings_output_path = s"$canonical_path/etljobs/src/test/resources/output/movies/ratings",
          ratings_output_dataset = "test",
          ratings_output_table_name = "ratings",
          ratings_output_file_name = Some("ratings.orc")
        )
      case EtlJob2CSVtoPARQUETtoBQLocalWith3Steps =>
        EtlJob23Props(
          job_name = EtlJob2CSVtoPARQUETtoBQLocalWith3Steps,
          job_properties = job_properties,
          ratings_input_path = s"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
          ratings_output_path = s"$canonical_path/etljobs/src/test/resources/output/movies/ratings",
          ratings_output_dataset = "test",
          ratings_output_table_name = "ratings_par"
        )
      case EtlJob3CSVtoCSVtoBQGcsWith2Steps =>
        EtlJob23Props(
          job_name = EtlJob3CSVtoCSVtoBQGcsWith2Steps,
          ratings_input_path        = f"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
          ratings_output_path       = f"gs://${global_properties.get.gcs_output_bucket}/output/ratings",
          ratings_output_dataset    = job_properties.getOrElse("ratings_output_dataset","test"),
          ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name","ratings_par")
        )
    }
  }
  def toEtlJob(job_name: MyEtlJobName, job_properties: EtlJobProps): EtlJob = {
    job_name match {
      case EtlJob1PARQUETtoORCtoBQLocalWith2Steps => etljob1.EtlJobDefinition(job_properties, global_properties)
      case EtlJob2CSVtoPARQUETtoBQLocalWith3Steps => etljob2.EtlJobDefinition(job_properties, global_properties)
      case EtlJob3CSVtoCSVtoBQGcsWith2Steps => new etljob3.EtlJobDefinition(job_properties, global_properties)
    }
  }
}
