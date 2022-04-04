package etlflow.etltask

import etlflow.spark.{SparkApi, SparkEnv}
import org.apache.spark.sql.SparkSession
import zio.{RIO, Task}

case class SparkTask[OP](name: String, transformFunction: SparkSession => OP) extends EtlTaskZIO[SparkEnv, OP] {
  override protected def processZio: RIO[SparkEnv, OP] =
    for {
      spark <- SparkApi.getSparkSession
      _ = logger.info("#" * 50)
      _ = logger.info(s"Starting Spark ETL Step: $name")
      op <- Task(transformFunction(spark))
      _ = logger.info("#" * 50)
    } yield op
}
