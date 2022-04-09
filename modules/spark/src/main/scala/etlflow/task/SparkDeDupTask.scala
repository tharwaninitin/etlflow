package etlflow.task

import etlflow.spark.{IOType, SparkApi, SparkEnv}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row}
import zio.{RIO, Task, ZIO}
import scala.reflect.runtime.universe.TypeTag

case class SparkDeDupTask[I <: Product: TypeTag](
    name: String,
    inputLocation: String,
    inputType: IOType,
    inputFilter: String = "1 = 1",
    transformation: Dataset[I] => Dataset[Row],
    checkpointLocation: String,
    eventTimeCol: String,
    delayThreshold: String,
    deDupCols: Seq[String]
) extends EtlTask[SparkEnv, Unit] {

  override protected def process: RIO[SparkEnv, Unit] = for {
    _ <- ZIO.succeed {
      logger.info("#" * 50)
      logger.info(s"Starting SparkDeDupTask: $name")
    }
    ip <- SparkApi.readStreamingDS[I](inputLocation, inputType, inputFilter)
    _ <- Task {
      ip.transform(transformation)
        .withWatermark(eventTimeCol, delayThreshold)
        .dropDuplicates(deDupCols)
        .writeStream
        .format("console")
        .outputMode("append")
        .trigger(Trigger.Once())
        .option("checkpointLocation", checkpointLocation)
        .start()
        .awaitTermination()
    }
    _ = logger.info("#" * 50)
  } yield ()
}
