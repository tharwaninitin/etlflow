package etlflow.etlsteps

import etlflow.spark.{IOType, ReadApi, WriteApi}
import etlflow.utils.LoggingLevel
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import zio.Task
import scala.reflect.runtime.universe.TypeTag

class SparkReadWriteStateStep[T <: Product: TypeTag, IPSTATE, O <: Product: TypeTag, OPSTATE] private[etlsteps] (
      val name : String
      ,input_location: Seq[String]
      ,input_type: IOType
      // ,input_columns: Seq[String] = Seq("*")
      ,input_filter: String = "1 = 1"
      ,output_location: String
      ,output_type: IOType
      ,output_filename: Option[String] = None
      ,output_partition_col: Seq[String] = Seq.empty[String]
      ,output_save_mode: SaveMode = SaveMode.Append
      ,output_repartitioning: Boolean = false
      ,transform_function: Either[
        Option[(SparkSession,DatasetWithState[T, IPSTATE]) => DatasetWithState[O, OPSTATE]],
        Option[(SparkSession,Dataset[T]) => Dataset[O]]
      ]
  )
  extends EtlStep[IPSTATE,OPSTATE] {
  private var recordsWrittenCount = 0L
  var spark: Option[SparkSession] = None

  final def process(input_state: =>IPSTATE): Task[OPSTATE] = Task {
    implicit lazy val sp = spark.get
    sp.sparkContext.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        synchronized {
          recordsWrittenCount += taskEnd.taskMetrics.outputMetrics.recordsWritten
        }
      }
    })
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting ETL Step : $name")
    val ds = ReadApi.LoadDS[T](input_location,input_type,input_filter)(sp)

    transform_function match {
      case Left(tf) =>
        tf match {
          case Some(transformFunc) =>
            val output = transformFunc(sp,DatasetWithState[T, IPSTATE](ds, input_state))
            WriteApi.WriteDS[O](output_type, output_location, output_partition_col, output_save_mode, output_filename, repartition=output_repartitioning)(output.ds,sp)
            etl_logger.info("#################################################################################################")
            output.state
          case None =>
            WriteApi.WriteDS[T](output_type, output_location, output_partition_col, output_save_mode, output_filename, repartition=output_repartitioning)(ds,sp)
            etl_logger.info("#################################################################################################")
            input_state.asInstanceOf[OPSTATE]
        }
      case Right(tf) =>
        tf match {
          case Some(transformFunc) =>
            val output = transformFunc(sp,ds)
            WriteApi.WriteDS[O](output_type, output_location, output_partition_col, output_save_mode, output_filename, repartition=output_repartitioning)(output,sp)
            etl_logger.info("#################################################################################################")
            input_state.asInstanceOf[OPSTATE]
          case None =>
            WriteApi.WriteDS[T](output_type, output_location, output_partition_col, output_save_mode, output_filename, repartition=output_repartitioning)(ds,sp)
            etl_logger.info("#################################################################################################")
            input_state.asInstanceOf[OPSTATE]
        }
    }
  }

  override def getStepProperties(level: LoggingLevel) : Map[String,String] = {
    val in_map = ReadApi.LoadDSHelper[T](level,input_location,input_type).toList
    val out_map = WriteApi.WriteDSHelper[O](level,output_type, output_location, output_partition_col, output_save_mode, output_filename, recordsWrittenCount, repartition=output_repartitioning).toList
    (in_map ++ out_map).toMap
  }

  override def getExecutionMetrics : Map[String, Map[String,String]] = {
    Map(name ->
      Map("Number of records written" -> recordsWrittenCount.toString)
    )
  }

  def showCorruptedData(): Unit = {
    etl_logger.info(s"Corrupted data for job $name:")
    val ds = ReadApi.LoadDS[T](input_location,input_type)(spark.get)
    ds.filter("_corrupt_record is not null").show(100,truncate = false)
  }
}

object SparkReadTransformWriteStateStep {
  def apply[T <: Product : TypeTag, IPSTATE, O <: Product : TypeTag, OPSTATE](
                                                                               name: String
                                                                               ,input_location: Seq[String]
                                                                               ,input_type: IOType
                                                                               //,input_columns: Seq[String] = Seq("*")
                                                                               ,input_filter: String = "1 = 1"
                                                                               ,output_location: String
                                                                               ,output_type: IOType
                                                                               ,output_filename: Option[String] = None
                                                                               ,output_partition_col: Seq[String] = Seq.empty[String]
                                                                               ,output_save_mode: SaveMode = SaveMode.Append
                                                                               ,output_repartitioning: Boolean = false
                                                                               ,transform_with_state: (SparkSession,DatasetWithState[T, IPSTATE]) => DatasetWithState[O, OPSTATE]
                                                                             ): SparkReadWriteStateStep[T, IPSTATE, O, OPSTATE] = {
    new SparkReadWriteStateStep[T, IPSTATE, O, OPSTATE](name, input_location, input_type, input_filter, output_location,
      output_type, output_filename, output_partition_col, output_save_mode, output_repartitioning, Left(Some(transform_with_state)))
  }
}

object SparkReadWriteStateStep {
  def apply[T <: Product : TypeTag, IPSTATE, OPSTATE](
                                                       name: String
                                                       ,input_location: Seq[String]
                                                       ,input_type: IOType
                                                       //,input_columns: Seq[String] = Seq("*")
                                                       ,input_filter: String = "1 = 1"
                                                       ,output_location: String
                                                       ,output_type: IOType
                                                       ,output_filename: Option[String] = None
                                                       ,output_partition_col: Seq[String] = Seq.empty[String]
                                                       ,output_save_mode: SaveMode = SaveMode.Append
                                                       ,output_repartitioning: Boolean = false
                                                     ): SparkReadWriteStateStep[T, IPSTATE, T, OPSTATE] = {
    new SparkReadWriteStateStep[T, IPSTATE, T, OPSTATE](name, input_location, input_type, input_filter, output_location,
      output_type, output_filename, output_partition_col, output_save_mode, output_repartitioning, Left(None))
  }
}