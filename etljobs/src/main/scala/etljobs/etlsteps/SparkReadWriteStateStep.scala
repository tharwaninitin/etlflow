package etljobs.etlsteps

import etljobs.etlsteps.SparkReadWriteStateStep.{Input, Output}
import etljobs.spark.{ReadApi, WriteApi}
import etljobs.utils.IOType
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Dataset, SparkSession, SaveMode}
import scala.util.Try
import scala.reflect.runtime.universe.TypeTag

class SparkReadWriteStateStep[T <: Product: TypeTag, IPSTATE, O <: Product: TypeTag, OPSTATE](
          val name : String
          ,input_location: Seq[String]
          ,input_type: IOType
          ,output_location: String
          ,output_type: IOType
          ,output_filename: Option[String] = None
          ,output_partition_col: Option[String] = None
          ,output_save_mode: SaveMode = SaveMode.Append
          ,output_repartitioning: Boolean = false
          ,transform_function : Option[Input[T, IPSTATE] => Output[O, OPSTATE]] = None
        )(spark : => SparkSession)
extends EtlStep[IPSTATE,OPSTATE]
{
  private var recordsWrittenCount = 0L
  implicit lazy val sp = spark

  def process(input_state: IPSTATE): Try[OPSTATE] = {
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
          val output = tf(Input[T, IPSTATE](ds, input_state))
          WriteApi.WriteDS[O](output_type, output_location, output_partition_col, output_save_mode, output_filename, repartition=output_repartitioning)(output.ds,sp)
          etl_logger.info("#################################################################################################")
          output.ops
        }
        case None => {
          WriteApi.WriteDS[T](output_type, output_location, output_partition_col, output_save_mode, output_filename, repartition=output_repartitioning)(ds,sp)
          etl_logger.info("#################################################################################################")
          input_state.asInstanceOf[OPSTATE]
        }
      }
    }

  }

  override def getStepProperties(level: String) : Map[String,String] = {
    val in_map = ReadApi.LoadDSHelper[T](level,input_location,input_type).toList
    val out_map = WriteApi.WriteDSHelper[O](level,output_type, output_location, output_partition_col, output_save_mode, output_filename, recordsWrittenCount,repartition=output_repartitioning).toList
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

object SparkReadWriteStateStep {
  case class Input[T, S](ds: Dataset[T], ips: S)
  case class Output[T, S](ds: Dataset[T], ops: S)

  def apply[T <: Product: TypeTag, IPSTATE, O <: Product: TypeTag, OPSTATE](
             name: String
             ,input_location: Seq[String]
             ,input_type: IOType
             ,output_location: String
             ,output_type: IOType
             ,output_filename: Option[String] = None
             ,output_partition_col: Option[String] = None
             ,output_save_mode: SaveMode = SaveMode.Append
             ,output_repartitioning: Boolean = false
             ,transform_function : Option[Input[T, IPSTATE] => Output[O, OPSTATE]] = None
           ) (spark: => SparkSession): SparkReadWriteStateStep[T,IPSTATE,O,OPSTATE] = {
    new SparkReadWriteStateStep[T,IPSTATE,O,OPSTATE](name,input_location,input_type,output_location,output_type,output_filename,output_partition_col,output_save_mode,output_repartitioning,transform_function)(spark)
  }
}