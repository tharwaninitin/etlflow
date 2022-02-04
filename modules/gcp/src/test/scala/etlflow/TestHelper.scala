package etlflow

trait TestHelper {
  lazy val gcp_project_id: Option[String] = sys.env.get("GCP_PROJECT_ID")
  lazy val gcp_region: Option[String]     = sys.env.get("GCP_REGION")

  lazy val gcs_bucket: String          = sys.env("GCS_BUCKET")
  lazy val gcs_input_location: String  = sys.env("GCS_INPUT_LOCATION")
  lazy val gcs_output_location: String = sys.env("GCS_OUTPUT_LOCATION")

  lazy val dp_cluster_name: String            = sys.env("DP_CLUSTER_NAME")
  lazy val dp_endpoint: String                = sys.env("DP_ENDPOINT")
  lazy val dp_bucket_name: String             = sys.env("DP_BUCKET_NAME")
  lazy val dp_subnet_uri: Option[String]      = sys.env.get("DP_SUBNET_URI")
  lazy val dp_network_tags: List[String]      = sys.env.get("DP_NETWORK_TAGS").map(_.split(",").toList).getOrElse(List.empty)
  lazy val dp_service_account: Option[String] = sys.env.get("DP_SERVICE_ACCOUNT")

  val canonical_path: String    = new java.io.File(".").getCanonicalPath
  val file_path_parquet: String = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"
  val file_path_csv: String     = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings/ratings_1.csv"
}
