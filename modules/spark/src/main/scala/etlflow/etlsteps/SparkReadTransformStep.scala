package etlflow.etlsteps

import etlflow.spark.IOType
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.reflect.runtime.universe.TypeTag

object SparkReadTransformStep {
  def apply[T <: Product : TypeTag, O <: Product : TypeTag](
     name: String
     ,input_location: Seq[String]
     ,input_type: IOType
     ,input_filter: String = "1 = 1"
     ,transform_function: (SparkSession,Dataset[T]) => Dataset[O]
   )(implicit spark: SparkSession): SparkReadStep[T, O] = {
    new SparkReadStep[T, O](name, input_location, input_type, input_filter, Some(transform_function))
  }
}
