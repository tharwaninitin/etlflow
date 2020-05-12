package etljobs.etlsteps

import org.apache.spark.sql.SparkSession
import zio.Task

class SparkETLStep (
                  val name: String
                  ,transform_function: SparkSession => Unit
                  )
extends EtlStep[SparkSession,Unit]
{
  def process(spark: SparkSession): Task[Unit] = Task {
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting ETL Step : $name")
    transform_function(spark)
    etl_logger.info("#################################################################################################")
  }
}

object SparkETLStep {
  def apply(name: String, transform_function: SparkSession => Unit): SparkETLStep =
    new SparkETLStep(name, transform_function)
}
