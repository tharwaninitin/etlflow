package etljobs.etljob3

import org.scalatest.{FlatSpec, Matchers}
import etljobs.MyGlobalProperties
import etljobs.schema.EtlJobList.EtlJob3CSVtoCSVtoBQGcsWith2Steps
import etljobs.schema.EtlJobProps.{EtlJob23Props}

class EtlJobTestSuite extends FlatSpec with Matchers {
  // AppLogger.initialize()
  //
  //val create_table_script = """
  //    CREATE TABLE test.ratings_par (
  //      user_id INT64
  //    , movie_id INT64
  //    , rating FLOAT64
  //    , timestamp INT64
  //    , date DATE
  //    ) PARTITION BY date
  //    """
  // STEP 1: Initialize job properties and create BQ tables required for jobs 
  private val canonical_path = new java.io.File(".").getCanonicalPath
  val global_props = new MyGlobalProperties(canonical_path + "/etljobs/src/test/resources/loaddata.properties")

  val job_props = EtlJob23Props(
    job_run_id = java.util.UUID.randomUUID.toString,
    job_name = EtlJob3CSVtoCSVtoBQGcsWith2Steps,
    ratings_input_path = f"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
    ratings_output_path = f"gs://${global_props.gcs_output_bucket}/output/ratings",
    ratings_output_dataset = "test",
    ratings_output_table_name = "ratings_par"
  )

  // STEP 2: Execute JOB
  val etljob = new EtlJobDefinition(job_properties=job_props, global_properties=Some(global_props))

  val thrown = intercept[Exception] {
    etljob.execute(send_slack_notification = true, log_in_db = true, notification_level = "info")
  }

  assert(thrown.getMessage === "Could not load data in FormatOptions{format=PARQUET} format in table test.ratings_par$20160102 due to error Error while reading data, error message: Input file is not in Parquet format.")
}


