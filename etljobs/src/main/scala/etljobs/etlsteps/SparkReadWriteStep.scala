package etljobs.etlsteps

import etljobs.spark.{ReadApi, WriteApi}
import etljobs.utils.IOType
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Dataset, SparkSession, SaveMode}
import scala.util.Try
import scala.reflect.runtime.universe.TypeTag

class SparkReadWriteStep[T <: Product: TypeTag, O <: Product: TypeTag](
          val name : String
          ,input_location: Seq[String]
          ,input_type: IOType
          ,output_location: String
          ,output_type: IOType
          ,output_filename: Option[String] = None
          ,output_partition_col: Option[String] = None
          ,output_save_mode: SaveMode = SaveMode.Append
          ,output_repartitioning: Boolean = false
          ,transform_function : Option[Dataset[T] => Dataset[O]] = None
        )(spark : => SparkSession, etl_metadata : Map[String, String])
extends EtlStep[Unit,Unit]
{
  private var recordsWrittenCount = 0L
  implicit lazy val sp = spark

  def process(input_state: Unit): Try[Unit] = {
    Try{
      spark.sparkContext.addSparkListener(new SparkListener() {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
          synchronized {
            recordsWrittenCount += taskEnd.taskMetrics.outputMetrics.recordsWritten
          }
        }
      })
      etl_logger.info("#################################################################################################")
      etl_logger.info(s"Starting ETL Step : $name")
      val ds = ReadApi.LoadDS[T](input_location,input_type)(sp)
      
      transform_function match {
        case Some(tf) => {
          val output = tf(ds)
          WriteApi.WriteDS[O](output_type, output_location, output_partition_col, output_save_mode, output_filename, repartition=output_repartitioning)(output,sp)
          etl_logger.info("#################################################################################################")
        }
        case None => {
          WriteApi.WriteDS[T](output_type, output_location, output_partition_col, output_save_mode, output_filename, repartition=output_repartitioning)(ds,sp)
          etl_logger.info("#################################################################################################")
        }
      }
    }

  }

  override def getStepProperties : Map[String,String] = {
    val in_map = ReadApi.LoadDSHelper[T](input_location,input_type).toList
    val out_map = transform_function match {
      case Some(_) => WriteApi.WriteDSHelper[O](output_type, output_location, output_partition_col, output_save_mode, output_filename, repartition=output_repartitioning).toList
      case None => WriteApi.WriteDSHelper[T](output_type, output_location, output_partition_col, output_save_mode, output_filename, repartition=output_repartitioning).toList
    }
    (in_map ++ out_map).toMap
  }

  override def getExecutionMetrics : Map[String, Map[String,String]] = {
    Map(name ->
      Map("Number of records written" -> recordsWrittenCount.toString)
    )
  }
  
  def showCorruptedData(): Unit = {
    etl_logger.info(s"Corrupted data for job $name:")
    val ds = ReadApi.LoadDS[T](input_location,input_type)(sp)
    ds.filter("_corrupt_record is not null").show(100,truncate = false)
  }
}

object SparkReadWriteStep {
  def apply[T <: Product: TypeTag, O <: Product: TypeTag](
             name: String
             ,input_location: Seq[String]
             ,input_type: IOType
             ,output_location: String
             ,output_type: IOType
             ,output_filename: Option[String] = None
             ,output_partition_col: Option[String] = None
             ,output_save_mode: SaveMode = SaveMode.Append
             ,output_repartitioning: Boolean = false
             ,transform_function : Option[Dataset[T] => Dataset[O]] = None
           ) (spark: => SparkSession, etl_metadata : Map[String, String]): SparkReadWriteStep[T,O] = {
    new SparkReadWriteStep[T,O](name,input_location,input_type,output_location,output_type,output_filename,output_partition_col,output_save_mode,output_repartitioning,transform_function)(spark,etl_metadata)
  }
}