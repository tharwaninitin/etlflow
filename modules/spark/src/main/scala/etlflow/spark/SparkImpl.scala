package etlflow.spark

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import zio.{Task, ZLayer}
import scala.reflect.runtime.universe.TypeTag

object SparkImpl {
  val live: ZLayer[SparkSession, Throwable, SparkApi] = ZLayer.fromFunction { spark: SparkSession =>
    new Service {
      override def LoadDSHelper[T <: Product: TypeTag](
          location: Seq[String],
          input_type: IOType,
          where_clause: String
      ): Task[Map[String, String]] = Task(ReadApi.LoadDSHelper[T](location, input_type))
      override def LoadDS[T <: Product: TypeTag](
          location: Seq[String],
          input_type: IOType,
          where_clause: String
      ): Task[Dataset[T]] = Task(ReadApi.LoadDS[T](location, input_type, where_clause)(spark))
      override def LoadDF(location: Seq[String], input_type: IOType, where_clause: String): Task[Dataset[Row]] = ???
    }
  }
}
