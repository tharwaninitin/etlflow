package etljobs.etlsteps

import etljobs.etlsteps.SparkReadWriteStateStep.{Input, Output}
import etljobs.spark.{ReadApi, WriteApi}
import etljobs.utils.IOType
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.util.Try
import scala.reflect.runtime.universe.TypeTag

class SparkReadWriteStateStep[T <: Product: TypeTag, IPSTATE, O <: Product: TypeTag, OPSTATE](
          val name : String
          ,input_location: Seq[String]
          ,input_type: IOType
          ,output_location: String
          ,output_type: IOType
          ,output_filename: Option[String] = None
          ,transform_function : Option[Input[T, IPSTATE] => Output[O, OPSTATE]] = None
        )(spark : => SparkSession, etl_metadata : Map[String, String])
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
      val ds = ReadApi.LoadDS[T](input_location,input_type)
      
      transform_function match {
        case Some(tf) => {
          val output = tf(Input[T, IPSTATE](ds, input_state))
          WriteApi.WriteDS[O](output_type,output_location,output_filename)(output.ds,spark)
          etl_logger.info("#################################################################################################")
          output.ops
        }
        case None => {
          WriteApi.WriteDS[T](output_type,output_location,output_filename)(ds,spark)
          etl_logger.info("#################################################################################################")
          input_state.asInstanceOf[OPSTATE]
        }
      }
    }

  }

  override def getStepProperties : Map[String,String] = {
    val in_map = ReadApi.LoadDSHelper[T](input_location,input_type).toList
    val out_map = WriteApi.WriteDSHelper[O](output_type,output_location,output_filename).toList
    (in_map ++ out_map).toMap
  }

  override def getExecutionMetrics : Map[String, Map[String,String]] = {
    Map(name ->
      Map("Number of records written" -> recordsWrittenCount.toString)
    )
  }
  
  def showCorruptedData(): Unit = {
    etl_logger.info(s"Corrupted data for job $name:")
    val ds = ReadApi.LoadDS[T](input_location,input_type)
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
             ,transform_function : Option[Input[T, IPSTATE] => Output[O, OPSTATE]] = None
           ) (spark: => SparkSession, etl_metadata : Map[String, String]): SparkReadWriteStateStep[T,IPSTATE,O,OPSTATE] = {
    new SparkReadWriteStateStep[T,IPSTATE,O,OPSTATE](name,input_location,input_type,output_location,output_type,output_filename,transform_function)(spark,etl_metadata)
  }
}