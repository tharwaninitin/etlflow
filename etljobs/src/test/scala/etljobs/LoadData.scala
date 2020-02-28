package etljobs

import etljobs.etljob3.EtlJobDefinition
import etljobs.schema.EtlJobList.{EtlJob3CSVtoPARQUETtoBQGcsWith2Steps, MyEtlJobName}
import etljobs.schema.EtlJobProps.EtlJob3Props
import scala.util.Try

object LoadData extends EtlJobApp[MyEtlJobName] {
  private val canonical_path = new java.io.File(".").getCanonicalPath
  val global_properties: Option[MyGlobalProperties] = Try(new MyGlobalProperties(s"$canonical_path/etljobs/src/test/resources/loaddata.properties")).toOption
  val send_notification = true
  val notification_level = "debug"

  def toEtlJob(job_name: String, job_properties: Map[String, String]): EtlJob = {
    val job3Props = EtlJob3Props(
      job_run_id = java.util.UUID.randomUUID.toString,
      job_name = EtlJob3CSVtoPARQUETtoBQGcsWith2Steps,
      ratings_input_path = f"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
      ratings_output_path = f"gs://${global_properties.get.gcs_output_bucket}/output/ratings",
      ratings_output_dataset = "test",
      ratings_output_table_name = "ratings_par"
    )
    job_name match {
      case "EtlJob3CSVtoPARQUETtoBQGcsWith2Steps" => new EtlJobDefinition(job_properties=Right(job3Props))
    }
  }
}
