package etlflow.spark

import org.apache.spark.sql.{Dataset, Row, SparkSession}
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
    def ReadDF(location: Seq[String], input_type: IOType, where_clause: String): F[Dataset[Row]]
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
  def ReadDF(location: Seq[String], input_type: IOType, where_clause: String = "1 = 1"): RIO[SparkEnv, Dataset[Row]] =
    ZIO.accessM[SparkEnv](_.get.ReadDF(location, input_type, where_clause))
}
