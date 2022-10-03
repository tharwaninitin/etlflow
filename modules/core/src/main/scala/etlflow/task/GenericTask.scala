package etlflow.task

import zio.{Task, ZIO}

class GenericTask[OP](val name: String, function: => OP) extends EtlTask[Any, OP] {

  override protected def process: Task[OP] = for {
    _  <- ZIO.logInfo("#" * 50) *> ZIO.logInfo(s"Starting Generic ETL Task: $name")
    op <- ZIO.attempt(function)
    _  <- ZIO.logInfo("#" * 50)
  } yield op
}

object GenericTask {
  def apply[OP](name: String, function: => OP): GenericTask[OP] = new GenericTask(name, function)
}
