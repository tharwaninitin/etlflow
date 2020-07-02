package etlflow.etlsteps

import etlflow.spark.SparkManager
import etlflow.utils.GlobalProperties
import org.apache.spark.sql.SparkSession
import zio.Task

class SparkETLStep[IP,OP] (
                  val name: String
                  ,transform_function: (SparkSession,IP) => OP
                  ,global_properties: Option[GlobalProperties] = None
                  )
extends EtlStep[IP,OP] with SparkManager
{
  lazy val spark: SparkSession = createSparkSession(global_properties)

  final def process(input_state: =>IP): Task[OP] = Task {
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting Spark ETL Step: $name")
    val op = transform_function(spark,input_state)
    etl_logger.info("#################################################################################################")
    op
  }
}

object SparkETLStep {
  def apply[IP,OP](
                    name: String,
                    transform_function: (SparkSession,IP) => OP,
                    global_properties: Option[GlobalProperties] = None
                  ): SparkETLStep[IP,OP] =
    new SparkETLStep[IP,OP](name, transform_function)
}
