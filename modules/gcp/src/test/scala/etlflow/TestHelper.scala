package etlflow

trait TestHelper {
  lazy val gcpProjectId: Option[String] = sys.env.get("GCP_PROJECT_ID")
  lazy val gcpRegion: Option[String]    = sys.env.get("GCP_REGION")

  lazy val gcsBucket: String         = sys.env("GCS_BUCKET")
  lazy val gcsInputLocation: String  = sys.env("GCS_INPUT_LOCATION")
  lazy val gcsOutputLocation: String = sys.env("GCS_OUTPUT_LOCATION")

  lazy val dpCluster: String                = sys.env("DP_CLUSTER_NAME")
  lazy val dpEndpoint: String               = sys.env("DP_ENDPOINT")
  lazy val dpBucket: String                 = sys.env("DP_BUCKET_NAME")
  lazy val dpSubnetUri: Option[String]      = sys.env.get("DP_SUBNET_URI")
  lazy val dpNetworkTags: List[String]      = sys.env.get("DP_NETWORK_TAGS").map(_.split(",").toList).getOrElse(List.empty)
  lazy val dpServiceAccount: Option[String] = sys.env.get("DP_SERVICE_ACCOUNT")

  lazy val bqDataset: String = sys.env("BQ_DATASET")
  lazy val bqTable: String   = sys.env("BQ_TABLE")

  val canonicalPath: String   = new java.io.File(".").getCanonicalPath
  val filePathParquet: String = s"$canonicalPath/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"
  val filePathCsv: String     = s"$canonicalPath/modules/core/src/test/resources/input/movies/ratings/ratings_1.csv"
}
