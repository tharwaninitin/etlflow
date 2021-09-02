package etlflow.etlsteps

import org.apache.spark.sql.SparkSession
import zio.Task

class SparkETLStep[IP,OP] (
    val name: String
    ,transform_function: (SparkSession,IP) => OP
    )(implicit spark: SparkSession)
extends EtlStep[IP,OP] {

  final def process(input_state: =>IP): Task[OP] = Task {
    logger.info("#################################################################################################")
    logger.info(s"Starting Spark ETL Step: $name")
    val op = transform_function(spark,input_state)
    logger.info("#################################################################################################")
    op
  }
}

object SparkETLStep {
  def apply[IP,OP](
                    name: String,
                    transform_function: (SparkSession,IP) => OP
                  )(implicit spark: SparkSession): SparkETLStep[IP,OP] =
    new SparkETLStep[IP,OP](name, transform_function)
}
