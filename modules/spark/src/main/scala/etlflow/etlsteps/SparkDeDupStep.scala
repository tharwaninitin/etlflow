package etlflow.etlsteps

import etlflow.spark.{IOType, SparkApi, SparkEnv}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row}
import zio.{RIO, Task, UIO}
import scala.reflect.runtime.universe.TypeTag

case class SparkDeDupStep[I <: Product: TypeTag](
    name: String,
    input_location: String,
    input_type: IOType,
    input_filter: String = "1 = 1",
    transformation: Dataset[I] => Dataset[Row],
    checkpointLocation: String,
    eventTimeCol: String,
    delayThreshold: String,
    deDupCols: Seq[String]
) extends EtlStep[SparkEnv, Unit] {

  protected def process: RIO[SparkEnv, Unit] = for {
    _ <- UIO {
      logger.info("#" * 50)
      logger.info(s"Starting SparkDeDupStep: $name")
    }
    ip <- SparkApi.readStreamingDS[I](input_location, input_type, input_filter)
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
