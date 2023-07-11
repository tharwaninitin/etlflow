package etlflow.task

import zio.{RIO, ZIO}

case class GenericTask[-R, +OP](name: String, task: RIO[R, OP]) extends EtlTask[R, OP] {

  override protected def process: RIO[R, OP] = for {
    _  <- ZIO.logInfo("#" * 50) *> ZIO.logInfo(s"Starting Generic ETL Task: $name")
    op <- task
    _  <- ZIO.logInfo("#" * 50)
  } yield op
}
