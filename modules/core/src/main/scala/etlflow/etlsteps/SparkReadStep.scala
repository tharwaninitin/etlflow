package etlflow.etlsteps

import etlflow.spark._
import etlflow.utils.IOType
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import zio.Task
import etlflow.utils.{HttpClientApi, JsonJackson, LoggingLevel}


import scala.reflect.runtime.universe.TypeTag

class SparkReadStep[I <: Product: TypeTag, O <: Product: TypeTag] private[etlsteps] (
                                                                                      val name: String
                                                                                      ,input_location: => Seq[String]
                                                                                      ,input_type: IOType
                                                                                      ,input_filter: String = "1 = 1"
                                                                                      ,transform_function: Option[(SparkSession,Dataset[I]) => Dataset[O]]
                                                                                    )(implicit spark: SparkSession)
  extends EtlStep[Unit,Dataset[O]] {
  private var recordsReadCount = 0L

  final def process(input_state: =>Unit): Task[Dataset[O]] = {
    val program = SparkApi.LoadDS[I](input_location,input_type,input_filter)

    spark.sparkContext.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        synchronized {
          recordsReadCount += taskEnd.taskMetrics.inputMetrics.recordsRead
        }
      }
    })
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting Spark Read Step : $name")

    transform_function match {
      case Some(transformFunc) =>
        program.map(ds => transformFunc(spark,ds)).provide(spark)
      case None =>
        val mapping = Encoders.product[O]
        program.map(ds => ds.as[O](mapping)).provide(spark)
    }
  }

  override def getStepProperties(level: LoggingLevel) : Map[String,String] = {
    ReadApi.LoadDSHelper[I](level,input_location,input_type).toList.toMap
  }

  override def getExecutionMetrics : Map[String, Map[String,String]] = {
    Map(name ->
      Map(
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

object SparkReadStep {
  def apply[T <: Product : TypeTag](
                                     name: String
                                     ,input_location: => Seq[String]
                                     ,input_type: IOType
                                     ,input_filter: String = "1 = 1"
                                   )(implicit spark: SparkSession): SparkReadStep[T, T] = {
    new SparkReadStep[T, T](name, input_location, input_type, input_filter, None)
  }
}