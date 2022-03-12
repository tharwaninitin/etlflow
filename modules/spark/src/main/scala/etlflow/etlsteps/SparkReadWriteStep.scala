package etlflow.etlsteps

import etlflow.spark._
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import zio.{RIO, UIO}
import scala.reflect.runtime.universe.TypeTag

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Throw"))
case class SparkReadWriteStep[I <: Product: TypeTag, O <: Product: TypeTag](
    name: String,
    input_location: List[String],
    input_type: IOType,
    input_filter: String = "1 = 1",
    output_location: String,
    output_type: IOType,
    output_save_mode: SaveMode = SaveMode.Append,
    output_partition_col: Seq[String] = Seq.empty[String],
    output_filename: Option[String] = None,
    output_compression: String = "none", // ("gzip","snappy")
    output_repartitioning: Boolean = false,
    output_repartitioning_num: Int = 1,
    transform_function: Option[(SparkSession, Dataset[I]) => Dataset[O]] = None
) extends EtlStep[SparkEnv, Unit] {

  private var recordsWrittenCount = 0L
  private var recordsReadCount    = 0L
  private var sparkRuntimeConf    = Map.empty[String, String]

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

  protected def process: RIO[SparkEnv, Unit] =
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
      ip <- SparkApi.readDS[I](input_location, input_type, input_filter)
      op <- transform_function match {
        case Some(transformFunc) =>
          SparkApi.writeDS[O](
            transformFunc(spark, ip),
            output_type,
            output_location,
            output_save_mode,
            output_partition_col,
            output_filename,
            output_compression,
            output_repartitioning,
            output_repartitioning_num
          ) *> UIO {
            logger.info(s"recordsReadCount: $recordsReadCount")
            logger.info(s"recordsWrittenCount: $recordsWrittenCount")
            logger.info("#" * 50)
          }
        case None =>
          SparkApi.writeDS[I](
            ip,
            output_type,
            output_location,
            output_save_mode,
            output_partition_col,
            output_filename,
            output_compression,
            output_repartitioning,
            output_repartitioning_num
          ) *> UIO {
            logger.info(s"recordsReadCount: $recordsReadCount")
            logger.info(s"recordsWrittenCount: $recordsWrittenCount")
            logger.info("#" * 50)
          }
      }
      _ = sparkRuntimeConf = SparkRuntimeConf(spark)
    } yield op

  override def getStepProperties: Map[String, String] = {
    val inMap = ReadApi.dSProps[I](input_location, input_type)
    val outMap = WriteApi.dSProps[O](
      output_type,
      output_location,
      output_save_mode,
      output_partition_col,
      output_filename,
      output_compression,
      output_repartitioning,
      output_repartitioning_num
    )
    inMap ++ outMap ++ sparkRuntimeConf ++ Map(
      "Number of records written" -> recordsWrittenCount.toString,
      "Number of records read"    -> recordsReadCount.toString
    )
  }

  def showCorruptedData(numRows: Int = 100): RIO[SparkEnv, Unit] = {
    logger.info(s"Corrupted data for job $name:")
    val program = SparkApi.readDS[O](input_location, input_type)
    program.map(_.filter("_corrupt_record is not null").show(numRows, truncate = false))
  }
}
