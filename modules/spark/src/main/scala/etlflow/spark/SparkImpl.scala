package etlflow.spark

import etlflow.utils.ApplicationLogger
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import zio.{Managed, Task, TaskLayer, UIO}
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag

final case class SparkImpl(spark: SparkSession) extends SparkApi.Service[Task] {

  override def getSparkSession: Task[SparkSession] = Task(spark)

  override def readDSProps[T <: Product: TypeTag](
      location: List[String],
      inputType: IOType,
      whereClause: String
  ): Task[Map[String, String]] =
    Task(ReadApi.dSProps[T](location, inputType))

  override def readDS[T <: Product: TypeTag](location: List[String], inputType: IOType, whereClause: String): Task[Dataset[T]] =
    Task(ReadApi.ds[T](location, inputType, whereClause)(spark))

  override def readStreamingDS[T <: Product: TypeTag](
      location: String,
      inputType: IOType,
      whereClause: String
  ): Task[Dataset[T]] =
    Task(ReadApi.streamingDS[T](location, inputType, whereClause)(spark))

  override def readDF(
      location: List[String],
      inputType: IOType,
      whereClause: String,
      selectClause: Seq[String]
  ): Task[Dataset[Row]] =
    Task(ReadApi.df(location, inputType, whereClause, selectClause)(spark))

  override def writeDSProps[T <: Product: universe.TypeTag](
      outputType: IOType,
      outputLocation: String,
      saveMode: SaveMode,
      partitionBy: Seq[String],
      outputFilename: Option[String],
      compression: String,
      repartition: Boolean,
      repartitionNo: Int
  ): Task[Map[String, String]] = Task(
    WriteApi.dSProps[T](
      outputType,
      outputLocation,
      saveMode,
      partitionBy,
      outputFilename,
      compression,
      repartition,
      repartitionNo
    )
  )

  override def writeDS[T <: Product: universe.TypeTag](
      input: Dataset[T],
      outputType: IOType,
      outputLocation: String,
      saveMode: SaveMode,
      partitionBy: Seq[String],
      outputFilename: Option[String],
      compression: String,
      repartition: Boolean,
      repartitionNo: Int
  ): Task[Unit] = Task(
    WriteApi.ds[T](
      input,
      outputType,
      outputLocation,
      saveMode,
      partitionBy,
      outputFilename,
      compression,
      repartition,
      repartitionNo
    )(spark)
  )
}

object SparkImpl extends ApplicationLogger {
  def live(spark: SparkSession): TaskLayer[SparkEnv] = Managed
    .make(Task(spark))(a =>
      UIO {
        logger.info("Stopping spark session")
        a.close()
      }
    )
    .map(SparkImpl(_))
    .toLayer
}
