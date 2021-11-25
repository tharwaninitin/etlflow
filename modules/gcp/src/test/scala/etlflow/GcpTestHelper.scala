package etlflow

trait GcpTestHelper {

  lazy val gcs_bucket: String = sys.env("GCS_BUCKET")
  lazy val gcs_input_location: String = sys.env("GCS_INPUT_LOCATION")
  lazy val gcs_output_location: String = sys.env("GCS_OUTPUT_LOCATION")
  lazy val pubsub_subscription: String = sys.env("PUBSUB_SUBSCRIPTION")
  lazy val gcp_project_id: String = sys.env("GCP_PROJECT_ID")

  val canonical_path: String = new java.io.File(".").getCanonicalPath
  val file = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"
  val file_csv = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings/ratings_1.csv"
}
