package etlflow.etlsteps

import etlflow.spark.{SparkApi, SparkEnv}
import org.apache.spark.sql.SparkSession
import zio.{RIO, Task}

case class SparkStep[OP](name: String, transformFunction: SparkSession => OP) extends EtlStep[SparkEnv, OP] {
  protected def process: RIO[SparkEnv, OP] =
    for {
      spark <- SparkApi.getSparkSession
      _ = logger.info("#" * 50)
      _ = logger.info(s"Starting Spark ETL Step: $name")
      op <- Task(transformFunction(spark))
      _ = logger.info("#" * 50)
    } yield op
}
