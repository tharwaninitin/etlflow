package etlflow

import etlflow.model.Credential.JDBC
import etlflow.spark.IOType.RDB

trait SparkTestSuiteHelper {
  val gcs_bucket             = sys.env.getOrElse("GCS_BUCKET", "GCS_BUCKET")
  val s3_bucket              = sys.env.getOrElse("S3_BUCKET", "GCS_BUCKET")
  val canonical_path: String = new java.io.File(".").getCanonicalPath
  val input_path_parquet     = s"$canonical_path/modules/spark/src/test/resources/input/ratings_parquet"
  val output_path            = s"$canonical_path/modules/spark/src/test/resources/output/ratings_json"
  val dataset_name           = "test"
  val table_name             = "ratings"
  val cred: JDBC             = JDBC("jdbc:postgresql://localhost:5432/etlflow", "etlflow", "etlflow", "org.postgresql.Driver")
  val jdbc: RDB              = RDB(cred)
  val ratings_intermediate_bucket: String = s"gs://$gcs_bucket/intermediate/ratings"
  val ratings_output_bucket_1: String     = s"gs://$gcs_bucket/output/ratings/csv"
  val ratings_output_bucket_2: String     = s"s3a://$s3_bucket/temp/output/ratings/parquet"
  val ratings_output_bucket_3: String     = s"gs://$gcs_bucket/output/ratings/json"
}
