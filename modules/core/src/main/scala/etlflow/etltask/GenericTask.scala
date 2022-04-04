package etlflow.etltask

import zio.{Task, ZIO}
import scala.util.Try

class GenericTask[OP](val name: String, function: => OP) extends EtlTaskTRY[OP] with EtlTaskZIO[Any, OP] {

  override protected def processTry: Try[OP] = Try {
    logger.info("#################################################################################################")
    logger.info(s"Starting Generic ETL Step: $name")
    val op = function
    logger.info("#################################################################################################")
    op
  }

  override protected def processZio: Task[OP] = ZIO.fromTry(processTry)
}

object GenericTask {
  def apply[OP](name: String, function: => OP): GenericTask[OP] = new GenericTask(name, function)
}
