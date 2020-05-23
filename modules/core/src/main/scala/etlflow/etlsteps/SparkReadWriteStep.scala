package etlflow.etlsteps

import etlflow.spark.{ReadApi, WriteApi}
import etlflow.utils.IOType
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import zio.Task
import scala.reflect.runtime.universe.TypeTag

class SparkReadWriteStep[I <: Product: TypeTag, O <: Product: TypeTag] private[etlsteps] (
          val name: String
          ,input_location: => Seq[String]
          ,input_type: IOType
          ,input_filter: String = "1 = 1"
          ,output_location: String
          ,output_type: IOType
          ,output_filename: Option[String] = None
          ,output_partition_col: Seq[String] = Seq.empty[String]
          ,output_save_mode: SaveMode = SaveMode.Append
          ,output_repartitioning: Boolean = false
          ,transform_function: Option[(SparkSession,Dataset[I]) => Dataset[O]]
        )
extends EtlStep[SparkSession,Unit] {
  private var recordsWrittenCount = 0L
  private var recordsReadCount = 0L

  final def process(spark: =>SparkSession): Task[Unit] = Task {
    val sp = spark
    sp.sparkContext.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        synchronized {
          recordsWrittenCount += taskEnd.taskMetrics.outputMetrics.recordsWritten
        }
      }
    })
    sp.sparkContext.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        synchronized {
          recordsReadCount += taskEnd.taskMetrics.inputMetrics.recordsRead
        }
      }
    })
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting ETL Step : $name")
    val ds = ReadApi.LoadDS[I](input_location,input_type,input_filter)(sp)

    transform_function match {
      case Some(transformFunc) =>
        val output = transformFunc(sp,ds)
        WriteApi.WriteDS[O](output_type, output_location, output_partition_col, output_save_mode, output_filename, repartition=output_repartitioning)(output,sp)
        etl_logger.info(s"recordsReadCount: $recordsReadCount")
        etl_logger.info(s"recordsWrittenCount: $recordsWrittenCount")
        etl_logger.info("#################################################################################################")
      case None =>
        WriteApi.WriteDS[I](output_type, output_location, output_partition_col, output_save_mode, output_filename, repartition=output_repartitioning)(ds,sp)
        etl_logger.info(s"recordsReadCount: $recordsReadCount")
        etl_logger.info(s"recordsWrittenCount: $recordsWrittenCount")
        etl_logger.info("#################################################################################################")
    }
  }

  override def getStepProperties(level: String) : Map[String,String] = {
    val in_map = ReadApi.LoadDSHelper[I](level,input_location,input_type).toList
    val out_map = WriteApi.WriteDSHelper[O](level,output_type, output_location, output_partition_col, output_save_mode, output_filename, recordsWrittenCount, repartition=output_repartitioning).toList
    (in_map ++ out_map).toMap
  }

  override def getExecutionMetrics : Map[String, Map[String,String]] = {
    Map(name ->
      Map(
        "Number of records written" -> recordsWrittenCount.toString,
        "Number of records read" -> recordsReadCount.toString
      )
    )
  }
  
//  def showCorruptedData(): Unit = {
//    etl_logger.info(s"Corrupted data for job $name:")
//    val ds = ReadApi.LoadDS[O](input_location,input_type)(spark)
//    ds.filter("_corrupt_record is not null").show(100,truncate = false)
//  }
}

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
           ,transform_function: (SparkSession,Dataset[T]) => Dataset[O]
         ): SparkReadWriteStep[T, O] = {
    new SparkReadWriteStep[T, O](name, input_location, input_type, input_filter, output_location,
      output_type, output_filename, output_partition_col, output_save_mode, output_repartitioning, Some(transform_function))
  }
}

object SparkReadWriteStep {
  def apply[T <: Product : TypeTag](
           name: String
           ,input_location: => Seq[String]
           ,input_type: IOType
           ,input_filter: String = "1 = 1"
           ,output_location: String
           ,output_type: IOType
           ,output_filename: Option[String] = None
           ,output_partition_col: Seq[String] = Seq.empty[String]
           ,output_save_mode: SaveMode = SaveMode.Append
           ,output_repartitioning: Boolean = false
         ): SparkReadWriteStep[T, T] = {
    new SparkReadWriteStep[T, T](name, input_location, input_type, input_filter, output_location,
      output_type, output_filename, output_partition_col, output_save_mode, output_repartitioning, None)
  }
}