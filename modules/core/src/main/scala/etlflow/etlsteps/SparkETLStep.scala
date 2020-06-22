package etlflow.etlsteps

import etlflow.spark.SparkManager
import etlflow.utils.GlobalProperties
import org.apache.spark.sql.SparkSession
import zio.Task

class SparkETLStep (
                  val name: String
                  ,transform_function: SparkSession => Unit
                  ,global_properties: Option[GlobalProperties] = None
                  )
extends EtlStep[Unit,Unit] with SparkManager
{
  lazy val spark: SparkSession = createSparkSession(global_properties)

  final def process(input_state: =>Unit): Task[Unit] = Task {
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting Spark ETL Step: $name")
    transform_function(spark)
    etl_logger.info("#################################################################################################")
  }
}

object SparkETLStep {
  def apply(name: String, transform_function: SparkSession => Unit, global_properties: Option[GlobalProperties] = None): SparkETLStep =
    new SparkETLStep(name, transform_function)
}
