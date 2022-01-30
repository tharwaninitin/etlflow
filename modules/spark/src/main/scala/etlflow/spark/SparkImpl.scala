package etlflow.spark

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import zio.{Managed, Task, TaskLayer}
import scala.reflect.runtime.universe.TypeTag

case class SparkImpl(spark: SparkSession) extends SparkApi.Service[Task] {
  override def getSparkSession: Task[SparkSession] = Task(spark)
  override def ReadDSProps[T <: Product: TypeTag](
      location: Seq[String],
      input_type: IOType,
      where_clause: String
  ): Task[Map[String, String]] =
    Task(ReadApi.DSProps[T](location, input_type))
  override def ReadDS[T <: Product: TypeTag](location: Seq[String], input_type: IOType, where_clause: String): Task[Dataset[T]] =
    Task(ReadApi.DS[T](location, input_type, where_clause)(spark))
  override def ReadDF(location: Seq[String], input_type: IOType, where_clause: String): Task[Dataset[Row]] =
    Task(ReadApi.DF(location, input_type, where_clause)(spark))
}
object SparkImpl {
  def live(spark: SparkSession): TaskLayer[SparkEnv] = Managed.fromAutoCloseable(Task(spark)).map(SparkImpl(_)).toLayer
}
