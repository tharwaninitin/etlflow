package etlflow.spark

import etlflow.utils.GlobalProperties
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

trait SparkManager  {
  private val ic_logger = LoggerFactory.getLogger(getClass.getName)

  def createSparkSession(gp: Option[GlobalProperties]): SparkSession = {
    ic_logger.info(f"======> Loaded SparkManager(${getClass.getName})")
    gp match {
      case Some(ss) =>
        if (ss.running_environment =="gcp") {
        if (SparkSession.getActiveSession.isDefined) {
          val spark = SparkSession.getActiveSession.get
          ic_logger.info("################# Using Already Created Spark Session ####################")
          spark
        }
        else {
          val spark = SparkSession.builder()
            .config("spark.default.parallelism", ss.spark_concurrent_threads)
            .config("spark.sql.shuffle.partitions", ss.spark_shuffle_partitions)
            .config("spark.sql.sources.partitionOverwriteMode", ss.spark_output_partition_overwrite_mode)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.hadoop.fs.s3a.access.key", ss.aws_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", ss.aws_secret_access_key)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
            .config("spark.hadoop.fs.s3a.fast.upload","true")
            .config("spark.sql.parquet.filterPushdown", "true")
            .config("spark.sql.parquet.mergeSchema", "false")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .config("spark.speculation", "false")
            .enableHiveSupport()
            .getOrCreate()

          ic_logger.info("##################### Created SparkSession ##########################")
          ic_logger.info("spark.scheduler.mode = " + spark.sparkContext.getSchedulingMode)
          ic_logger.info("spark.sparkContext.uiWebUrl = " + spark.sparkContext.uiWebUrl)
          ic_logger.info("spark.sparkContext.applicationId = " + spark.sparkContext.applicationId)
          ic_logger.info("spark.sparkContext.sparkUser = " + spark.sparkContext.sparkUser)
          ic_logger.info("spark.eventLog.dir = " + spark.conf.getOption("spark.eventLog.dir"))
          ic_logger.info("spark.eventLog.enabled = " + spark.conf.getOption("spark.eventLog.enabled"))
          spark.conf.getAll.filter(m1 => m1._1.contains("yarn")).foreach(kv => ic_logger.info(kv._1 + " = " + kv._2))
          ic_logger.info("#####################################################################")

          spark
        }
      }
        else if (ss.running_environment =="aws") {
          if (SparkSession.getActiveSession.isDefined) {
            ic_logger.info("################# Using Already Created Spark Session ####################")
            SparkSession.getActiveSession.get
          }
          else {
            val spark = SparkSession.builder()
              .config("spark.default.parallelism", ss.spark_concurrent_threads)
              .config("spark.sql.shuffle.partitions", ss.spark_shuffle_partitions)
              .config("spark.scheduler.mode", "FAIR")
              .config("spark.sql.sources.partitionOverwriteMode", ss.spark_output_partition_overwrite_mode)
              .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
              .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
              .config("fs.gs.project.id", ss.gcp_project)
              .config("fs.gs.auth.service.account.enable", "true")
              .config("google.cloud.auth.service.account.json.keyfile",ss.gcp_credential_file_path)
              .enableHiveSupport()
              .getOrCreate()

            ic_logger.info("##################### Created SparkSession ##########################")
            ic_logger.info("spark.scheduler.mode = " + spark.sparkContext.getSchedulingMode)
            ic_logger.info("spark.sparkContext.uiWebUrl = " + spark.sparkContext.uiWebUrl)
            ic_logger.info("spark.sparkContext.applicationId = " + spark.sparkContext.applicationId)
            ic_logger.info("spark.sparkContext.sparkUser = " + spark.sparkContext.sparkUser)
            ic_logger.info("spark.eventLog.dir = " + spark.conf.getOption("spark.eventLog.dir"))
            ic_logger.info("spark.eventLog.enabled = " + spark.conf.getOption("spark.eventLog.enabled"))
            spark.conf.getAll.filter(m1 => m1._1.contains("yarn")).foreach(kv => ic_logger.info(kv._1 + " = " + kv._2))
            ic_logger.info("#####################################################################")

            spark
          }
        }
        else if (ss.running_environment == "local") {
          if (SparkSession.getActiveSession.isDefined) {
            ic_logger.info("################# Using Already Created Local Spark Session ####################")
            SparkSession.getActiveSession.get
          }
          else {
            val spark = SparkSession.builder().master("local[*]")
              .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
              .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
              .config("spark.scheduler.mode", "FAIR")
              .config("fs.gs.project.id", ss.gcp_project)
              .config("fs.gs.auth.service.account.enable", "true")
              .config("google.cloud.auth.service.account.json.keyfile",ss.gcp_credential_file_path)
              .getOrCreate()
            spark.conf.set("credentialsFile", ss.gcp_credential_file_path)
            ic_logger.info("################### Created Local SparkSession ########################")
            ic_logger.info("spark.scheduler.mode = " + spark.sparkContext.getSchedulingMode)
            ic_logger.info("spark.sparkContext.uiWebUrl = " + spark.sparkContext.uiWebUrl)
            ic_logger.info("spark.sparkContext.applicationId = " + spark.sparkContext.applicationId)
            ic_logger.info("spark.sparkContext.sparkUser = " + spark.sparkContext.sparkUser)
            ic_logger.info("spark.eventLog.dir = " + spark.conf.getOption("spark.eventLog.dir"))
            ic_logger.info("spark.eventLog.enabled = " + spark.conf.getOption("spark.eventLog.enabled"))
            spark.conf.getAll.filter(m1 => m1._1.contains("yarn")).foreach(kv => ic_logger.info(kv._1 + " = " + kv._2))
            ic_logger.info("#####################################################################")

            spark
          }
        }
        else {
          throw new Exception("Exception occurred! Please provide correct value of property running_environment in loaddata.properties. Expected values are gcp or aws or local")
        }
      case None =>
        val spark = SparkSession.builder().master("local[*]")
          .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
          .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
          .config("spark.scheduler.mode", "FAIR")
          .getOrCreate()
        ic_logger.info("##################### Created Local SparkSession without GlobalProperties ##########################")
        ic_logger.info("spark.scheduler.mode = " + spark.sparkContext.getSchedulingMode)
        ic_logger.info("spark.sparkContext.uiWebUrl = " + spark.sparkContext.uiWebUrl)
        ic_logger.info("spark.sparkContext.applicationId = " + spark.sparkContext.applicationId)
        ic_logger.info("spark.sparkContext.sparkUser = " + spark.sparkContext.sparkUser)
        ic_logger.info("spark.eventLog.dir = " + spark.conf.getOption("spark.eventLog.dir"))
        ic_logger.info("spark.eventLog.enabled = " + spark.conf.getOption("spark.eventLog.enabled"))
        spark.conf.getAll.filter(m1 => m1._1.contains("yarn")).foreach(kv => ic_logger.info(kv._1 + " = " + kv._2))
        ic_logger.info("#####################################################################")
        spark
    }
  }
}
