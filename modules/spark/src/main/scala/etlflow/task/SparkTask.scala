package etlflow.task

import etlflow.spark.{SparkApi, SparkEnv}
import org.apache.spark.sql.SparkSession
import zio.{RIO, Task}

case class SparkTask[OP](name: String, transformFunction: SparkSession => OP) extends EtlTask[SparkEnv, OP] {
  override protected def process: RIO[SparkEnv, OP] =
    for {
      spark <- SparkApi.getSparkSession
      _ = logger.info("#" * 50)
      _ = logger.info(s"Starting Spark ETL Task: $name")
      op <- ZIO.attempt(transformFunction(spark))
      _ = logger.info("#" * 50)
    } yield op
}
