package etljobs.etlsteps

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Dataset, SparkSession}
import etljobs.etlsteps.ReadWriteTypedStateSparkStep.{Input,Output}
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

class ReadWriteTypedStateSparkStep[T, IPSTATE, O, OPSTATE](
          val name : String
          ,input_dataset : => Dataset[T]
          ,transform_function : Input[T, IPSTATE] => Output[O, OPSTATE]
          ,write_function : (Dataset[O],SparkSession) => Unit
        )(implicit spark : SparkSession, etl_metadata : Map[String, String])
extends EtlStep[IPSTATE,OPSTATE]
{
  private var recordsWrittenCount = 0L
  spark.sparkContext.addSparkListener(new SparkListener() {
    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
      synchronized {
        recordsWrittenCount += taskEnd.taskMetrics.outputMetrics.recordsWritten
      }
    }
  })

//  def map[U](func: OPSTATE => U): U = {
//    etl_logger.info("#################################################################################################")
//    val output = transform_function(Input[T, IPSTATE](input_dataset, input_state))
//    write_function(output.ds,spark)
//    etl_logger.info("#################################################################################################")
//    func(output.ops)
//  }
//
//  def flatMap[U](func: OPSTATE => U): U = {
//    etl_logger.info("#################################################################################################")
//    val output = transform_function(Input[T, IPSTATE](input_dataset, input_state))
//    write_function(output.ds,spark)
//    etl_logger.info("#################################################################################################")
//    func(output.ops)
//  }
//  def flatMap[U](func: OPSTATE => U): OPSTATE = {
//    val output = transform_function(Input[T, IPSTATE](source_dataset, input_state))
//    output
//  }
//  def map(func: Input[T, IPSTATE] => OPSTATE): OPSTATE = {
//    val output = transform_function(Input[T, IPSTATE](source_dataset, input_state))
//    output
//  }
//  def foreach(func: Input[T, IPSTATE] => Unit) : Unit = {
//    transform_function(Input[T, IPSTATE](input_dataset, input_state))
//  }

  def process(input_state: IPSTATE): Try[OPSTATE] = {
    Try{
      etl_logger.info("#################################################################################################")
      etl_logger.info(s"Starting ETL Step : $name")
      val output = transform_function(Input[T, IPSTATE](input_dataset, input_state))
      write_function(output.ds,spark)
      output.ops
    }

  }

  override def getStepProperties : Map[String,String] = {
    Map()
  }

  override def getExecutionMetrics : Map[String, Map[String,String]] = {
    Map(name ->
      Map("Number of records written" -> recordsWrittenCount.toString)
    )
  }
  
  def showCorruptedData(): Unit = {
    etl_logger.info(s"Corrupted data for job $name:")
    input_dataset.filter("_corrupt_record is not null").show(100,truncate = false)
  }
}

object ReadWriteTypedStateSparkStep {
  case class Input[T, S](ds: Dataset[T], ips: S)
  case class Output[T, S](ds: Dataset[T], ops: S)

  def apply[T <: Product: TypeTag, IPSTATE, O <: Product: TypeTag, OPSTATE](
    name: String
   , input_dataset : => Dataset[T]
   , transform_function: Input[T, IPSTATE] => Output[O, OPSTATE]
   , write_function: (Dataset[O], SparkSession) => Unit
  ) (implicit spark: SparkSession, etl_metadata : Map[String, String]): ReadWriteTypedStateSparkStep[T,IPSTATE,O,OPSTATE] = {
    new ReadWriteTypedStateSparkStep[T,IPSTATE,O,OPSTATE](name, input_dataset, transform_function, write_function)
  }
}