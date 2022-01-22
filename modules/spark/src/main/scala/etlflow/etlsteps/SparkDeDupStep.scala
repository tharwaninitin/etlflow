package etlflow.etlsteps

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.Trigger
import zio.Task
import scala.reflect.runtime.universe.TypeTag

case class SparkDeDupStep[I: TypeTag](
    name: String,
    inputDataStream: Dataset[I],
    checkpointLocation: String,
    eventTimeCol: String,
    delayThreshold: String,
    deDupCols: Seq[String]
) extends EtlStep[Unit] {

  final def process: Task[Unit] = Task {
    logger.info("#################################################################################################")
    logger.info(s"Starting SparkDeDupStep: $name")

    inputDataStream
      .withWatermark(eventTimeCol, delayThreshold)
      .dropDuplicates(deDupCols)
      .writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.Once())
      .option("checkpointLocation", checkpointLocation)
      .start()
      .awaitTermination()

    logger.info("#################################################################################################")
  }
}
