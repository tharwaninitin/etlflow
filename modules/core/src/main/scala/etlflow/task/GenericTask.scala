package etlflow.task

import zio.{Task, ZIO}
import scala.util.Try

class GenericTask[OP](val name: String, function: => OP) extends EtlTask[Any, OP] {

  override protected def processTry: Try[OP] = Try {
    logger.info("#################################################################################################")
    logger.info(s"Starting Generic ETL Task: $name")
    val op = function
    logger.info("#################################################################################################")
    op
  }

  override protected def process: Task[OP] = ZIO.fromTry(processTry)
}

object GenericTask {
  def apply[OP](name: String, function: => OP): GenericTask[OP] = new GenericTask(name, function)
}
