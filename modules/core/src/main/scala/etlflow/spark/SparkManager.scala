package etlflow.spark

import etlflow.utils.Environment
import etlflow.utils.Environment.{AWS, GCP, LOCAL}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkManager {
  private val spark_logger = LoggerFactory.getLogger(getClass.getName)
  def createSparkSession(
                          env: Set[Environment] = Set(LOCAL),
                          props: Map[String, String] = Map(
                            "spark.scheduler.mode" -> "FAIR",
                            "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
                            "spark.default.parallelism" -> "10",
                            "spark.sql.shuffle.partitions" -> "10"
                          )
                        ): SparkSession =  {
    if (SparkSession.getActiveSession.isDefined) {
      val spark = SparkSession.getActiveSession.get
      spark_logger.info(s"###### Using Already Created Spark Session with $env support ##########")
      spark
    }
    else {
      spark_logger.info(s"###### Creating Spark Session with $env support ##########")
      var sparkBuilder = SparkSession.builder()

      props.foreach{prop =>
        sparkBuilder = sparkBuilder.config(prop._1,prop._2)
      }

      env.foreach {
        case GCP(service_account_key_path, project_id) =>
          sparkBuilder = sparkBuilder
            .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            .config("fs.gs.project.id", project_id)
            .config("fs.gs.auth.service.account.enable", "true")
            .config("google.cloud.auth.service.account.json.keyfile", service_account_key_path)
            .config("credentialsFile", service_account_key_path)
            .enableHiveSupport()
        case AWS(access_key, secret_key) =>
          sparkBuilder = sparkBuilder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .enableHiveSupport()
        case LOCAL =>
          sparkBuilder = sparkBuilder
            .config("spark.ui.enabled", "false")
            .master("local[*]")
      }

      val spark = sparkBuilder.getOrCreate()
      spark
    }
  }
}
