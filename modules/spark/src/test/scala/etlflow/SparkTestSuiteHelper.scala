package etlflow

import etlflow.model.Credential.JDBC
import etlflow.spark.IOType.RDB

trait SparkTestSuiteHelper {
  val gcsBucket             = sys.env.getOrElse("GCS_BUCKET", "GCS_BUCKET")
  val s3Bucket              = sys.env.getOrElse("S3_BUCKET", "GCS_BUCKET")
  val canonicalPath: String = new java.io.File(".").getCanonicalPath
  val inputPathParquet      = s"$canonicalPath/modules/spark/src/test/resources/input/ratings_parquet"
  val outputPath            = s"$canonicalPath/modules/spark/src/test/resources/output/ratings_json"
  val datasetName           = "test"
  val tableName             = "ratings"
  val cred: JDBC            = JDBC("jdbc:postgresql://localhost:5432/etlflow", "etlflow", "etlflow", "org.postgresql.Driver")
  val jdbc: RDB             = RDB(cred)
  val ratingsIntermediateBucket: String = s"gs://$gcsBucket/intermediate/ratings"
  val ratingsOutputBucket1: String      = s"gs://$gcsBucket/output/ratings/csv"
  val ratingsOutputBucket2: String      = s"s3a://$s3Bucket/temp/output/ratings/parquet"
  val ratingsOutputBucket3: String      = s"gs://$gcsBucket/output/ratings/json"
}
