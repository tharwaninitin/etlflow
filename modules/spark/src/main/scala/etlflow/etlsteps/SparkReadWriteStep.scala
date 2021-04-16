package etlflow.etlsteps

import etlflow.spark.{IOType, ReadApi, WriteApi}
import etlflow.utils.LoggingLevel
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
       ,output_repartitioning_num: Int = 1
       ,transform_function: Option[(SparkSession,Dataset[I]) => Dataset[O]]
     )(implicit spark: SparkSession)
  extends EtlStep[Unit,Unit] {
  private var recordsWrittenCount = 0L
  private var recordsReadCount = 0L

  output_filename match {
    case Some(_) =>
      if (output_repartitioning_num != 1 || !output_repartitioning || output_partition_col.nonEmpty)
        throw new RuntimeException(
          s"""Error in step $name, output_filename option can only be used when
             |output_repartitioning is set to true and
             |output_repartitioning_num is set to 1
             |output_partition_col is empty""".stripMargin
        )
    case None =>
  }

  final def process(input_state: =>Unit): Task[Unit] = Task {
    spark.sparkContext.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        synchronized {
          recordsWrittenCount += taskEnd.taskMetrics.outputMetrics.recordsWritten
        }
      }
    })
    spark.sparkContext.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        synchronized {
          recordsReadCount += taskEnd.taskMetrics.inputMetrics.recordsRead
        }
      }
    })
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting ETL Step : $name")
    val ds = ReadApi.LoadDS[I](input_location,input_type,input_filter)(spark)

    transform_function match {
      case Some(transformFunc) =>
        val output = transformFunc(spark,ds)
        WriteApi.WriteDS[O](
          output_type, output_location, output_partition_col, output_save_mode, output_filename,
          repartition=output_repartitioning, n= output_repartitioning_num
        )(output,spark)
        etl_logger.info(s"recordsReadCount: $recordsReadCount")
        etl_logger.info(s"recordsWrittenCount: $recordsWrittenCount")
        etl_logger.info("#################################################################################################")
      case None =>
        WriteApi.WriteDS[I](
          output_type, output_location, output_partition_col, output_save_mode, output_filename,
          repartition=output_repartitioning, n= output_repartitioning_num
        )(ds,spark)
        etl_logger.info(s"recordsReadCount: $recordsReadCount")
        etl_logger.info(s"recordsWrittenCount: $recordsWrittenCount")
        etl_logger.info("#################################################################################################")
    }
  }

  override def getStepProperties(level: LoggingLevel) : Map[String,String] = {
    val in_map = ReadApi.LoadDSHelper[I](level,input_location,input_type).toList
    val out_map = WriteApi.WriteDSHelper[O](
      level,output_type, output_location, output_partition_col,
      output_save_mode, output_filename, recordsWrittenCount,
      repartition=output_repartitioning
    ).toList
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

  def showCorruptedData(): Unit = {
    etl_logger.info(s"Corrupted data for job $name:")
    val ds = ReadApi.LoadDS[O](input_location,input_type)(spark)
    ds.filter("_corrupt_record is not null").show(100,truncate = false)
  }
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
         ,output_repartitioning_num: Int = 1
         ,transform_function: (SparkSession,Dataset[T]) => Dataset[O]
       )(implicit spark: SparkSession): SparkReadWriteStep[T, O] = {
    new SparkReadWriteStep[T, O](name, input_location, input_type, input_filter, output_location,
      output_type, output_filename, output_partition_col, output_save_mode, output_repartitioning,
      output_repartitioning_num, Some(transform_function))
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
         ,output_repartitioning_num: Int = 1
       )(implicit spark: SparkSession): SparkReadWriteStep[T, T] = {
    new SparkReadWriteStep[T, T](name, input_location, input_type, input_filter, output_location,
      output_type, output_filename, output_partition_col, output_save_mode, output_repartitioning,
      output_repartitioning_num, None)
  }
}