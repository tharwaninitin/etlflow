package etlflow.etlsteps

import etlflow.etlsteps.DeltaLakeStep.DeltaLakeCmd
import etlflow.utils.LoggingLevel
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import zio.Task

class DeltaLakeStep (
                  val name: String,
                  val command: DeltaLakeCmd
                )(implicit spark: SparkSession)
  extends EtlStep[Unit,Unit] {

  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#" * 100)
    etl_logger.info(s"Starting DeltaLake Step: $name")
    etl_logger.info(s"Operation to perform: $command")
    command match {
      case DeltaLakeCmd.VACCUME(location, retention_period) => Task(deltaVaaccume(location,retention_period)(spark))
      case DeltaLakeCmd.HISTORY(location, limit) => Task(deltaHistory(location, limit)(spark))
    }
  }

  private def deltaVaaccume(location: String, retention_period:Double)(implicit spark: SparkSession):Unit = {
    val deltaTable = DeltaTable.forPath(spark,location)
    deltaTable.vacuum(retention_period)
  }

  private def deltaHistory(location: String, limit:Int)(implicit spark: SparkSession):Unit = {
    val deltaTable = DeltaTable.forPath(spark, location)
    deltaTable.history(limit).show()
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("operation_type" -> command.toString)
}

object DeltaLakeStep {
  sealed trait DeltaLakeCmd
  object DeltaLakeCmd {
    case class VACCUME(location: String, retention_period:Double) extends DeltaLakeCmd
    case class HISTORY(location: String, limit: Int = 0) extends DeltaLakeCmd
  }

  def apply(name: String, command: DeltaLakeCmd)(implicit spark: SparkSession): DeltaLakeStep = new DeltaLakeStep(name, command)
}
