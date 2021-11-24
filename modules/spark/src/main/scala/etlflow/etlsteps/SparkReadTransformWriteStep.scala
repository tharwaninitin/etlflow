package etlflow.etlsteps

import etlflow.spark.IOType
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import scala.reflect.runtime.universe.TypeTag

object SparkReadTransformWriteStep {
  def apply[T <: Product : TypeTag, O <: Product : TypeTag](
     name: String
     ,input_location: Seq[String]
     ,input_type: IOType
     ,input_filter: String = "1 = 1"
     ,output_location: String
     ,output_type: IOType
     ,output_filename: Option[String] = None
     ,output_partition_col: Seq[String] = Seq.empty[String]
     ,output_save_mode: SaveMode = SaveMode.Append
     ,output_repartitioning: Boolean = false
     ,output_repartitioning_num: Int = 1
     ,transform_function: (SparkSession,Dataset[T]) => Dataset[O]
   )(implicit spark: SparkSession): SparkReadWriteStep[T, O] = {
    new SparkReadWriteStep[T, O](name, input_location, input_type, input_filter, output_location,
      output_type, output_filename, output_partition_col, output_save_mode, output_repartitioning,
      output_repartitioning_num, Some(transform_function))
  }
}
