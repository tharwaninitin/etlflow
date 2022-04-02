package etlflow.etlsteps

import zio.Task

import scala.util.Try

class GenericETLStep[OP](val name: String, function: => OP) extends EtlStep[OP] {
  override protected type R = Any

  override protected def processTry: Try[OP] = Try {
    logger.info("#################################################################################################")
    logger.info(s"Starting Generic ETL Step: $name")
    val op = function
    logger.info("#################################################################################################")
    op
  }

  override protected def process: Task[OP] = Task.fromTry(processTry)
}

object GenericETLStep {
  def apply[OP](name: String, function: => OP): GenericETLStep[OP] = new GenericETLStep(name, function)
}
