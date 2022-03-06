package etlflow.etlsteps

import etlflow.spark._
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import zio.RIO
import scala.reflect.runtime.universe.TypeTag

case class SparkReadStep[I <: Product: TypeTag, O <: Product: TypeTag](
    name: String,
    input_location: Seq[String],
    input_type: IOType,
    input_filter: String = "1 = 1",
    transform_function: Option[(SparkSession, Dataset[I]) => Dataset[O]] = None
) extends EtlStep[SparkEnv, Dataset[O]] {

  private var recordsWrittenCount = 0L
  private var recordsReadCount    = 0L
  private var sparkRuntimeConf    = Map.empty[String, String]

  protected def process: RIO[SparkEnv, Dataset[O]] =
    for {
      spark <- SparkApi.getSparkSession
      _ = logger.info("#" * 50)
      _ = logger.info(s"Starting Spark Read Step: $name")
      _ = spark.sparkContext.addSparkListener(new SparkListener() {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit =
          synchronized {
            recordsReadCount += taskEnd.taskMetrics.inputMetrics.recordsRead
          }
      })
      _ = spark.sparkContext.addSparkListener(new SparkListener() {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit =
          synchronized {
            recordsWrittenCount += taskEnd.taskMetrics.outputMetrics.recordsWritten
          }
      })
      ip <- SparkApi.ReadDS[I](input_location, input_type, input_filter)
      op = transform_function match {
        case Some(transformFunc) => transformFunc(spark, ip)
        case None                => ip.as[O](Encoders.product[O])
      }
      _ = sparkRuntimeConf = SparkRuntimeConf(spark)
    } yield op

  override def getStepProperties: Map[String, String] =
    ReadApi.DSProps[I](input_location, input_type).toList.toMap ++ sparkRuntimeConf ++ Map(
      "Number of records written" -> recordsWrittenCount.toString,
      "Number of records read"    -> recordsReadCount.toString
    )

  def showCorruptedData(numRows: Int = 100): RIO[SparkEnv, Unit] = {
    logger.info(s"Corrupted data for job $name:")
    val program = SparkApi.ReadDS[O](input_location, input_type)
    program.map(_.filter("_corrupt_record is not null").show(numRows, truncate = false))
  }
}
