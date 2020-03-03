package etljobs

import etljobs.schema.EtlJobList.{EtlJob1PARQUETtoORCtoBQLocalWith2StepsWithSlack, EtlJob2CSVtoPARQUETtoBQLocalWith3Steps, EtlJob3CSVtoPARQUETtoBQGcsWith2Steps, MyEtlJobName}
import etljobs.schema.EtlJobProps.{EtlJob1Props, EtlJob23Props}
import scala.util.Try

object LoadData extends EtlJobApp[MyEtlJobName] {
  private val canonical_path = new java.io.File(".").getCanonicalPath
  val global_properties: Option[MyGlobalProperties] = Try(new MyGlobalProperties(s"$canonical_path/etljobs/src/test/resources/loaddata.properties")).toOption
  val send_notification = true
  val notification_level = "debug"

  def toEtlJob(job_name: MyEtlJobName, job_properties: Map[String, String]): EtlJob = {
    lazy val job1Props = EtlJob1Props(
      job_run_id = java.util.UUID.randomUUID.toString,
      job_name = EtlJob1PARQUETtoORCtoBQLocalWith2StepsWithSlack,
      ratings_input_path = s"$canonical_path/etljobs/src/test/resources/input/movies/ratings_parquet/*",
      ratings_output_path = s"$canonical_path/etljobs/src/test/resources/output/movies/ratings",
      ratings_output_dataset = "test",
      ratings_output_table_name = "ratings",
      ratings_output_file_name = "ratings.parquet"
    )

    job_name match {
      case EtlJob1PARQUETtoORCtoBQLocalWith2StepsWithSlack =>
        new etljob1.EtlJobDefinition(
          job_properties = job1Props
        )
      case EtlJob2CSVtoPARQUETtoBQLocalWith3Steps =>
        new etljob2.EtlJobDefinition(
          job_properties = EtlJob23Props(
            job_run_id = java.util.UUID.randomUUID.toString,
            job_name = EtlJob2CSVtoPARQUETtoBQLocalWith3Steps,
            ratings_input_path = s"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
            ratings_output_path = s"$canonical_path/etljobs/src/test/resources/output/movies/ratings",
            ratings_output_dataset = "test",
            ratings_output_table_name = "ratings_par"
          )
        )
      case EtlJob3CSVtoPARQUETtoBQGcsWith2Steps =>
        new etljob3.EtlJobDefinition(
          job_properties = EtlJob23Props(
            job_run_id = java.util.UUID.randomUUID.toString,
            job_name = EtlJob3CSVtoPARQUETtoBQGcsWith2Steps,
            ratings_input_path = f"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
            ratings_output_path = f"gs://${global_properties.get.gcs_output_bucket}/output/ratings",
            ratings_output_dataset = "test",
            ratings_output_table_name = "ratings_par"
          )
        )
    }
  }
}
