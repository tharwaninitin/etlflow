package etlflow.spark

import etlflow.log.ApplicationLogger
import etlflow.spark.Environment.LOCAL
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import zio.{Task, TaskLayer, ZIO, ZLayer}
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag

final case class SparkLive(spark: SparkSession) extends SparkApi.Service[Task] {

  override def getSparkSession: Task[SparkSession] = ZIO.attempt(spark)

  override def readDSProps[T <: Product: TypeTag](
      location: List[String],
      inputType: IOType,
      whereClause: String
  ): Task[Map[String, String]] =
    ZIO.attempt(ReadApi.dSProps[T](location, inputType))

  override def readDS[T <: Product: TypeTag](location: List[String], inputType: IOType, whereClause: String): Task[Dataset[T]] =
    ZIO.attempt(ReadApi.ds[T](location, inputType, whereClause)(spark))

  override def readStreamingDS[T <: Product: TypeTag](
      location: String,
      inputType: IOType,
      whereClause: String
  ): Task[Dataset[T]] =
    ZIO.attempt(ReadApi.streamingDS[T](location, inputType, whereClause)(spark))

  override def readDF(
      location: List[String],
      inputType: IOType,
      whereClause: String,
      selectClause: Seq[String]
  ): Task[Dataset[Row]] =
    ZIO.attempt(ReadApi.df(location, inputType, whereClause, selectClause)(spark))

  override def writeDSProps[T <: Product: universe.TypeTag](
      outputType: IOType,
      outputLocation: String,
      saveMode: SaveMode,
      partitionBy: Seq[String],
      outputFilename: Option[String],
      compression: String,
      repartition: Boolean,
      repartitionNo: Int
  ): Task[Map[String, String]] = ZIO.attempt(
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
  ): Task[Unit] = ZIO.attempt(
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

object SparkLive extends ApplicationLogger {
  def live(spark: SparkSession): TaskLayer[SparkEnv] = ZLayer.scoped {
    ZIO
      .acquireRelease(ZIO.attempt(spark))(a =>
        ZIO.succeed {
          logger.info("Stopping spark session")
          a.close()
        }
      )
      .map(SparkLive(_))
  }

  def live(
      env: Set[Environment] = Set(LOCAL),
      props: Map[String, String] = Map(
        "spark.scheduler.mode"                     -> "FAIR",
        "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
        "spark.default.parallelism"                -> "10",
        "spark.sql.shuffle.partitions"             -> "10"
      ),
      hiveSupport: Boolean = false
  ): TaskLayer[SparkEnv] = ZLayer.scoped {
    ZIO
      .acquireRelease(ZIO.attempt(SparkManager.createSparkSession(env, props, hiveSupport)))(a =>
        ZIO.succeed {
          logger.info("Stopping spark session")
          a.close()
        }
      )
      .map(SparkLive(_))
  }
}
