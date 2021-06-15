package etlflow.spark

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import Environment._

object SparkManager {
  private val spark_logger = LoggerFactory.getLogger(getClass.getName)
  private def showSparkProperties(spark: SparkSession): Unit = {
    spark_logger.info("spark.scheduler.mode = " + spark.sparkContext.getSchedulingMode)
    spark_logger.info("spark.default.parallelism = " + spark.conf.getOption("spark.default.parallelism"))
    spark_logger.info("spark.sql.shuffle.partitions = " + spark.conf.getOption("spark.sql.shuffle.partitions"))
    spark_logger.info("spark.sql.sources.partitionOverwriteMode = " + spark.conf.getOption("spark.sql.sources.partitionOverwriteMode"))
    spark_logger.info("spark.sparkContext.uiWebUrl = " + spark.sparkContext.uiWebUrl)
    spark_logger.info("spark.sparkContext.applicationId = " + spark.sparkContext.applicationId)
    spark_logger.info("spark.sparkContext.sparkUser = " + spark.sparkContext.sparkUser)
    spark_logger.info("spark.eventLog.dir = " + spark.conf.getOption("spark.eventLog.dir"))
    spark_logger.info("spark.eventLog.enabled = " + spark.conf.getOption("spark.eventLog.enabled"))
    spark.conf.getAll.filter(m1 => m1._1.contains("yarn")).foreach(kv => spark_logger.info(kv._1 + " = " + kv._2))
  }

  def createSparkSession(
                          env: Set[Environment] = Set(LOCAL),
                          props: Map[String, String] = Map(
                            "spark.scheduler.mode" -> "FAIR",
                            "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
                            "spark.default.parallelism" -> "10",
                            "spark.sql.shuffle.partitions" -> "10",
                            "spark.sql.extensions"-> "io.delta.sql.DeltaSparkSessionExtension",
                            "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                            "spark.databricks.delta.retentionDurationCheck.enabled"-> "false"
                          ),
                          hive_support: Boolean = true
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
        case AWS(access_key, secret_key) =>
          sparkBuilder = sparkBuilder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        case LOCAL =>
          sparkBuilder = sparkBuilder
            .config("spark.ui.enabled", "false")
            .master("local[*]")
      }

      if (hive_support) sparkBuilder = sparkBuilder.enableHiveSupport()

      val spark = sparkBuilder.getOrCreate()
      showSparkProperties(spark)
      spark
    }
  }
}