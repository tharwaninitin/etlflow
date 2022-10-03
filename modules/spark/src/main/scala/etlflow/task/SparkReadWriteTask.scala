package etlflow.task

import etlflow.spark._
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import zio.{RIO, UIO}
import scala.reflect.runtime.universe.TypeTag

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Throw"))
case class SparkReadWriteTask[I <: Product: TypeTag, O <: Product: TypeTag](
    name: String,
    inputLocation: List[String],
    inputType: IOType,
    inputFilter: String = "1 = 1",
    outputLocation: String,
    outputType: IOType,
    outputSaveMode: SaveMode = SaveMode.Append,
    outputPartitionCol: Seq[String] = Seq.empty[String],
    outputFilename: Option[String] = None,
    outputCompression: String = "none", // ("gzip","snappy")
    outputRepartitioning: Boolean = false,
    outputRepartitioningNum: Int = 1,
    transformFunction: Option[(SparkSession, Dataset[I]) => Dataset[O]] = None
) extends EtlTask[SparkEnv, Unit] {

  private var recordsWrittenCount = 0L
  private var recordsReadCount    = 0L
  private var sparkRuntimeConf    = Map.empty[String, String]

  outputFilename match {
    case Some(_) =>
      if (outputRepartitioningNum != 1 || !outputRepartitioning || outputPartitionCol.nonEmpty)
        throw new RuntimeException(
          s"""Error in task $name, output_filename option can only be used when
             |output_repartitioning is set to true and
             |output_repartitioning_num is set to 1
             |output_partition_col is empty""".stripMargin
        )
    case None =>
  }

  override protected def process: RIO[SparkEnv, Unit] =
    for {
      spark <- SparkApi.getSparkSession
      _ = logger.info("#" * 50)
      _ = logger.info(s"Starting Spark Read Task: $name")
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
      ip <- SparkApi.readDS[I](inputLocation, inputType, inputFilter)
      op <- transformFunction match {
        case Some(transformFunc) =>
          SparkApi.writeDS[O](
            transformFunc(spark, ip),
            outputType,
            outputLocation,
            outputSaveMode,
            outputPartitionCol,
            outputFilename,
            outputCompression,
            outputRepartitioning,
            outputRepartitioningNum
          ) *> ZIO.succeed {
            logger.info(s"recordsReadCount: $recordsReadCount")
            logger.info(s"recordsWrittenCount: $recordsWrittenCount")
            logger.info("#" * 50)
          }
        case None =>
          SparkApi.writeDS[I](
            ip,
            outputType,
            outputLocation,
            outputSaveMode,
            outputPartitionCol,
            outputFilename,
            outputCompression,
            outputRepartitioning,
            outputRepartitioningNum
          ) *> ZIO.succeed {
            logger.info(s"recordsReadCount: $recordsReadCount")
            logger.info(s"recordsWrittenCount: $recordsWrittenCount")
            logger.info("#" * 50)
          }
      }
      _ = sparkRuntimeConf = SparkRuntimeConf(spark)
    } yield op

  override def getTaskProperties: Map[String, String] = {
    val inMap = ReadApi.dSProps[I](inputLocation, inputType)
    val outMap = WriteApi.dSProps[O](
      outputType,
      outputLocation,
      outputSaveMode,
      outputPartitionCol,
      outputFilename,
      outputCompression,
      outputRepartitioning,
      outputRepartitioningNum
    )
    inMap ++ outMap ++ sparkRuntimeConf ++ Map(
      "Number of records written" -> recordsWrittenCount.toString,
      "Number of records read"    -> recordsReadCount.toString
    )
  }

  def showCorruptedData(numRows: Int = 100): RIO[SparkEnv, Unit] = {
    logger.info(s"Corrupted data for job $name:")
    val program = SparkApi.readDS[O](inputLocation, inputType)
    program.map(_.filter("_corrupt_record is not null").show(numRows, truncate = false))
  }
}
