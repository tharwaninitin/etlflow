package etlflow.etlsteps

import org.apache.spark.sql.SparkSession
import zio.Task

class SparkETLStep[OP](val name: String, transform_function: SparkSession => OP)(implicit spark: SparkSession)
    extends EtlStep[Any, OP] {

  final def process: Task[OP] = Task {
    logger.info("#################################################################################################")
    logger.info(s"Starting Spark ETL Step: $name")
    val op = transform_function(spark)
    logger.info("#################################################################################################")
    op
  }
}

object SparkETLStep {
  def apply[OP](
      name: String,
      transform_function: SparkSession => OP
  )(implicit spark: SparkSession): SparkETLStep[OP] = new SparkETLStep[OP](name, transform_function)
}
