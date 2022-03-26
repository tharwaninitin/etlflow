package etlflow

import etlflow.spark.Environment.{AWS, GCP, LOCAL}
import etlflow.spark.SparkManager
import org.apache.spark.sql.SparkSession

trait TestSparkSession {
  lazy val spark: SparkSession = SparkManager.createSparkSession(
    hiveSupport = false,
    env = Set(
      LOCAL,
      GCP(
        sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", "/path/not/set/to/credential/file"),
        sys.env.getOrElse("GCP_PROJECT_ID", "<---->")
      ),
      AWS(sys.env.getOrElse("ACCESS_KEY", "<---->"), sys.env.getOrElse("SECRET_KEY", "<---->"))
    )
  )
}
