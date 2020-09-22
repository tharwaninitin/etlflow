package etlflow

import etlflow.utils.Environment.{AWS, GCP, LOCAL}
import etlflow.utils.{Environment, IOType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import zio.{Has, Task, ZIO, ZLayer}
import scala.reflect.runtime.universe.TypeTag

package object spark {
  private val spark_logger = LoggerFactory.getLogger(getClass.getName)
  type SparkApi = Has[Service]
  trait Service {
    def LoadDSHelper[T <: Product : TypeTag](level: String,location: Seq[String], input_type: IOType, where_clause: String = "1 = 1"): Task[Map[String,String]]
    def LoadDS[T <: Product : TypeTag](location: Seq[String], input_type: IOType, where_clause: String = "1 = 1"): Task[Dataset[T]]
    def LoadDF(location: Seq[String], input_type: IOType, where_clause: String = "1 = 1"): Task[Dataset[Row]]
  }
  object SparkApi {
    lazy val env: ZLayer[SparkSession, Throwable, SparkApi] = SparkImpl.live

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
                              "spark.sql.shuffle.partitions" -> "10"
                            )
                          ): Task[SparkSession] = Task {
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
        showSparkProperties(spark)
        spark
      }
    }

    def LoadDS[T <: Product : TypeTag](location: Seq[String], input_type: IOType, where_clause: String = "1 = 1"): ZIO[SparkSession, Throwable, Dataset[T]] =
      ZIO.accessM[SparkApi](_.get.LoadDS[T](location,input_type,where_clause)).provideLayer(env)
  }
}
