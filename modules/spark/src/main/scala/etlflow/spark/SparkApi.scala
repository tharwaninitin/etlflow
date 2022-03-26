package etlflow.spark

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import zio.{RIO, ZIO}
import scala.reflect.runtime.universe.TypeTag

object SparkApi {

  trait Service[F[_]] {
    def getSparkSession: F[SparkSession]
    def readDSProps[T <: Product: TypeTag](location: List[String], inputType: IOType, whereClause: String): F[Map[String, String]]
    def readDS[T <: Product: TypeTag](location: List[String], inputType: IOType, whereClause: String): F[Dataset[T]]
    def readStreamingDS[T <: Product: TypeTag](location: String, inputType: IOType, whereClause: String): F[Dataset[T]]
    def readDF(location: List[String], inputType: IOType, whereClause: String, selectClause: Seq[String]): F[Dataset[Row]]
    def writeDSProps[T <: Product: TypeTag](
        outputType: IOType,
        outputLocation: String,
        saveMode: SaveMode,
        partitionBy: Seq[String],
        outputFilename: Option[String],
        compression: String,
        repartition: Boolean,
        repartitionNo: Int
    ): F[Map[String, String]]
    def writeDS[T <: Product: TypeTag](
        input: Dataset[T],
        outputType: IOType,
        outputLocation: String,
        saveMode: SaveMode,
        partitionBy: Seq[String],
        outputFilename: Option[String],
        compression: String,
        repartition: Boolean,
        repartitionNo: Int
    ): F[Unit]
  }

  def getSparkSession: RIO[SparkEnv, SparkSession] = ZIO.accessM[SparkEnv](_.get.getSparkSession)
  def readDSProps[T <: Product: TypeTag](
      location: List[String],
      inputType: IOType,
      whereClause: String
  ): RIO[SparkEnv, Map[String, String]] = ZIO.accessM[SparkEnv](_.get.readDSProps[T](location, inputType, whereClause))
  def readDS[T <: Product: TypeTag](
      location: List[String],
      inputType: IOType,
      whereClause: String = "1 = 1"
  ): RIO[SparkEnv, Dataset[T]] = ZIO.accessM[SparkEnv](_.get.readDS[T](location, inputType, whereClause))
  def readStreamingDS[T <: Product: TypeTag](
      location: String,
      inputType: IOType,
      whereClause: String = "1 = 1"
  ): RIO[SparkEnv, Dataset[T]] = ZIO.accessM[SparkEnv](_.get.readStreamingDS[T](location, inputType, whereClause))
  def readDF(
      location: List[String],
      inputType: IOType,
      whereClause: String = "1 = 1",
      selectClause: Seq[String] = Seq("*")
  ): RIO[SparkEnv, Dataset[Row]] =
    ZIO.accessM[SparkEnv](_.get.readDF(location, inputType, whereClause, selectClause))
  def writeDSProps[T <: Product: TypeTag](
      outputType: IOType,
      outputLocation: String,
      saveMode: SaveMode = SaveMode.Append,
      partitionBy: Seq[String] = Seq.empty[String],
      outputFilename: Option[String] = None,
      compression: String = "none",
      repartition: Boolean = false,
      repartitionNo: Int = 1
  ): RIO[SparkEnv, Map[String, String]] =
    ZIO.accessM[SparkEnv](
      _.get.writeDSProps(
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
  def writeDS[T <: Product: TypeTag](
      input: Dataset[T],
      outputType: IOType,
      outputLocation: String,
      saveMode: SaveMode = SaveMode.Append,
      partitionBy: Seq[String] = Seq.empty[String],
      outputFilename: Option[String] = None,
      compression: String = "none",
      repartition: Boolean = false,
      repartitionNo: Int = 1
  ): RIO[SparkEnv, Unit] = ZIO.accessM[SparkEnv](
    _.get.writeDS(
      input,
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
}
