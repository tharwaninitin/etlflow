package etlflow

import etlflow.model.Credential.JDBC
import etlflow.spark.IOType.RDB

trait SparkTestSuiteHelper {
  val canonical_path: String = new java.io.File(".").getCanonicalPath
  val input_path_parquet     = s"$canonical_path/modules/spark/src/test/resources/input/ratings_parquet"
  val output_path            = s"$canonical_path/modules/spark/src/test/resources/output/ratings_json"
  val output_table           = "ratings"
  val cred: JDBC             = JDBC("jdbc:postgresql://localhost:5432/etlflow", "etlflow", "etlflow", "org.postgresql.Driver")
  val jdbc: RDB              = RDB(cred)
}
