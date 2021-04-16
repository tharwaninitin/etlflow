package etlflow

import ch.qos.logback.classic.{Level, Logger => LBLogger}
import etlflow.spark.SparkManager
import etlflow.spark.Environment.{AWS, GCP, LOCAL}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

trait TestSparkSession {

  lazy val spark_logger: LBLogger = LoggerFactory.getLogger("org.apache.spark").asInstanceOf[LBLogger]
  lazy val spark_jetty_logger: LBLogger = LoggerFactory.getLogger("org.spark_project.jetty").asInstanceOf[LBLogger]

  spark_logger.setLevel(Level.WARN)
  spark_jetty_logger.setLevel(Level.WARN)

  implicit val spark: SparkSession = SparkManager.createSparkSession(
    hive_support = false,
    env = Set(
      LOCAL,
      GCP(sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS","/path/not/set/to/credential/file"),sys.env.getOrElse("GCP_PROJECT_ID","<---->")),
      AWS(sys.env.getOrElse("ACCESS_KEY","<---->"),sys.env.getOrElse("SECRET_KEY","<---->"))
    )
  )
}
