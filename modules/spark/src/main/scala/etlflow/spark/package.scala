package etlflow

import etlflow.utils.LoggingLevel
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import zio.{Has, Task, ZIO, ZLayer}
import scala.reflect.runtime.universe.TypeTag

package object spark {
  private val spark_logger = LoggerFactory.getLogger(getClass.getName)
  type SparkApi = Has[Service]
  sealed trait IOType extends Serializable
  object IOType {
    final case class CSV(delimiter: String = ",", header_present: Boolean = true, parse_mode: String = "FAILFAST", quotechar: String = "\"") extends IOType {
      override def toString: String = s"CSV with delimiter => $delimiter header_present => $header_present parse_mode => $parse_mode"
    }
    final case class MCSV(delimiter: String, no_of_columns: Int) extends IOType
    final case class JSON(multi_line: Boolean = false) extends IOType {
      override def toString: String = s"Json with multiline  => $multi_line"
    }
    final case class JDBC(url: String, user: String, password: String, driver: String) extends IOType {
      override def toString: String = s"JDBC with url => $url"
    }
    final case class BQ(temp_dataset: String = "temp", operation_type: String = "table") extends IOType
    final case object PARQUET extends IOType
    final case object ORC extends IOType
    final case object TEXT extends IOType
    final case object EXCEL extends IOType
  }

  sealed trait Environment
  object Environment {
    final case class GCP(service_account_key_path: String, project_id: String = "") extends Environment {
      override def toString: String = "****service_account_key_path****"
    }
    final case class AWS(access_key: String, secret_key: String) extends Environment {
      override def toString: String = "****access_key****secret_key****"
    }
    case object LOCAL extends Environment
  }

  trait Service {
    def LoadDSHelper[T <: Product : TypeTag](level: LoggingLevel,location: Seq[String], input_type: IOType, where_clause: String = "1 = 1"): Task[Map[String,String]]
    def LoadDS[T <: Product : TypeTag](location: Seq[String], input_type: IOType, where_clause: String = "1 = 1"): Task[Dataset[T]]
    def LoadDF(location: Seq[String], input_type: IOType, where_clause: String = "1 = 1"): Task[Dataset[Row]]
  }
  object SparkApi {
    import Environment._
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
