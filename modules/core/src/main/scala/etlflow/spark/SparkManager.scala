package etlflow.spark

import etlflow.utils.GlobalProperties
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

trait SparkManager  {
  private val ic_logger = LoggerFactory.getLogger(getClass.getName)

  private def showSparkProperties(spark: SparkSession): Unit = {
    ic_logger.info("spark.scheduler.mode = " + spark.sparkContext.getSchedulingMode)
    ic_logger.info("spark.sparkContext.uiWebUrl = " + spark.sparkContext.uiWebUrl)
    ic_logger.info("spark.sparkContext.applicationId = " + spark.sparkContext.applicationId)
    ic_logger.info("spark.sparkContext.sparkUser = " + spark.sparkContext.sparkUser)
    ic_logger.info("spark.eventLog.dir = " + spark.conf.getOption("spark.eventLog.dir"))
    ic_logger.info("spark.eventLog.enabled = " + spark.conf.getOption("spark.eventLog.enabled"))
    spark.conf.getAll.filter(m1 => m1._1.contains("yarn")).foreach(kv => ic_logger.info(kv._1 + " = " + kv._2))
  }

  def createSparkSession(gp: Option[GlobalProperties]): SparkSession = {
    ic_logger.info(f"======> Loaded SparkManager(${getClass.getName})")
    gp match {
      case Some(ss) =>
        if (SparkSession.getActiveSession.isDefined) {
          val spark = SparkSession.getActiveSession.get
          ic_logger.info(s"###### Using Already Created Spark Session in ${ss.running_environment} mode ##########")
          spark
        }
        else {
          ic_logger.info(s"###### Created Spark Session in ${ss.running_environment} mode ##########")
          val sparkBuilder = SparkSession.builder()
            .config("spark.default.parallelism", ss.spark_concurrent_threads)
            .config("spark.sql.shuffle.partitions", ss.spark_shuffle_partitions)
            .config("spark.scheduler.mode", ss.spark_scheduler_mode)
            .config("spark.sql.sources.partitionOverwriteMode", ss.spark_output_partition_overwrite_mode)

          if (ss.running_environment == "gcp") {
            val spark = sparkBuilder
              .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
              .config("spark.hadoop.fs.s3a.access.key", ss.aws_access_key)
              .config("spark.hadoop.fs.s3a.secret.key", ss.aws_secret_key)
              .enableHiveSupport()
              .getOrCreate()
            showSparkProperties(spark)
            spark
          }
          else if (ss.running_environment == "aws") {
            val spark = sparkBuilder
              .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
              .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
              .config("fs.gs.project.id", ss.gcp_project)
              .config("fs.gs.auth.service.account.enable", "true")
              .config("google.cloud.auth.service.account.json.keyfile", ss.gcp_credential_file_path)
              .enableHiveSupport()
              .getOrCreate()
            spark.conf.set("credentialsFile", ss.gcp_credential_file_path)
            showSparkProperties(spark)
            spark
          }
          else if (ss.running_environment == "local") {
            val spark = sparkBuilder
              .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
              .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
              .config("fs.gs.project.id", ss.gcp_project)
              .config("google.cloud.auth.service.account.json.keyfile", ss.gcp_credential_file_path)
              .config("fs.gs.auth.service.account.enable", "true")
              .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
              .config("spark.hadoop.fs.s3a.access.key", ss.aws_access_key)
              .config("spark.hadoop.fs.s3a.secret.key", ss.aws_secret_key)
              .master("local[*]")
              .getOrCreate()
            spark.conf.set("credentialsFile", ss.gcp_credential_file_path)
            showSparkProperties(spark)
            spark
          }
          else {
            throw new Exception("Exception occurred! Please provide correct value of property running_environment in loaddata.properties. Expected values are gcp or aws or local")
          }
        }
      case None =>
        ic_logger.info("########### Created Local SparkSession without GlobalProperties ############")
        val spark = SparkSession.builder().master("local[*]")
          .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
          .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
          .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
          .config("spark.scheduler.mode", "FAIR")
          .getOrCreate()
        showSparkProperties(spark)
        spark
    }
  }
}
