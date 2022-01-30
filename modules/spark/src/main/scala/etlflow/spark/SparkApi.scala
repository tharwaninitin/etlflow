package etlflow.spark

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import zio.{RIO, ZIO}
import scala.reflect.runtime.universe.TypeTag

object SparkApi {

  trait Service[F[_]] {
    def getSparkSession: F[SparkSession]
    def ReadDSProps[T <: Product: TypeTag](
        location: Seq[String],
        input_type: IOType,
        where_clause: String
    ): F[Map[String, String]]
    def ReadDS[T <: Product: TypeTag](location: Seq[String], input_type: IOType, where_clause: String): F[Dataset[T]]
    def ReadDF(location: Seq[String], input_type: IOType, where_clause: String, select_clause: Seq[String]): F[Dataset[Row]]
    def WriteDSProps[T <: Product: TypeTag](
        output_type: IOType,
        output_location: String,
        save_mode: SaveMode,
        partition_by: Seq[String],
        output_filename: Option[String],
        compression: String,
        repartition: Boolean,
        repartition_no: Int
    ): F[Map[String, String]]
    def WriteDS[T <: Product: TypeTag](
        input: Dataset[T],
        output_type: IOType,
        output_location: String,
        save_mode: SaveMode,
        partition_by: Seq[String],
        output_filename: Option[String],
        compression: String,
        repartition: Boolean,
        repartition_no: Int
    )(spark: SparkSession): F[Unit]
  }

  def getSparkSession: RIO[SparkEnv, SparkSession] = ZIO.accessM[SparkEnv](_.get.getSparkSession)
  def ReadDSProps[T <: Product: TypeTag](
      location: Seq[String],
      input_type: IOType,
      where_clause: String
  ): RIO[SparkEnv, Map[String, String]] = ZIO.accessM[SparkEnv](_.get.ReadDSProps[T](location, input_type, where_clause))
  def ReadDS[T <: Product: TypeTag](
      location: Seq[String],
      input_type: IOType,
      where_clause: String = "1 = 1"
  ): RIO[SparkEnv, Dataset[T]] = ZIO.accessM[SparkEnv](_.get.ReadDS[T](location, input_type, where_clause))
  def ReadDF(
      location: Seq[String],
      input_type: IOType,
      where_clause: String = "1 = 1",
      select_clause: Seq[String] = Seq("*")
  ): RIO[SparkEnv, Dataset[Row]] =
    ZIO.accessM[SparkEnv](_.get.ReadDF(location, input_type, where_clause, select_clause))
  def WriteDSProps[T <: Product: TypeTag](
      output_type: IOType,
      output_location: String,
      save_mode: SaveMode = SaveMode.Append,
      partition_by: Seq[String] = Seq.empty[String],
      output_filename: Option[String] = None,
      compression: String = "none",
      repartition: Boolean = false,
      repartition_no: Int = 1
  ): RIO[SparkEnv, Map[String, String]] =
    ZIO.accessM[SparkEnv](
      _.get.WriteDSProps(
        output_type,
        output_location,
        save_mode,
        partition_by,
        output_filename,
        compression,
        repartition,
        repartition_no
      )
    )
  def WriteDS[T <: Product: TypeTag](
      input: Dataset[T],
      output_type: IOType,
      output_location: String,
      save_mode: SaveMode = SaveMode.Append,
      partition_by: Seq[String] = Seq.empty[String],
      output_filename: Option[String] = None,
      compression: String = "none", // ("compression", "gzip","snappy")
      repartition: Boolean = false,
      repartition_no: Int = 1
  )(spark: SparkSession): RIO[SparkEnv, Unit] = ZIO.accessM[SparkEnv](
    _.get.WriteDS(
      input,
      output_type,
      output_location,
      save_mode,
      partition_by,
      output_filename,
      compression,
      repartition,
      repartition_no
    )(spark)
  )
}
