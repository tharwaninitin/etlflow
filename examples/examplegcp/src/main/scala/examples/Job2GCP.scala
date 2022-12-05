package examples

import etlflow.log.ApplicationLogger
import etlflow.task._
import zio._

object Job2GCP extends ZIOAppDefault with ApplicationLogger {
  override val bootstrap       = zioSlf4jLogger
  override def run: Task[Unit] = GenericTask("Hello", logger.info("Hello World")).execute.provide(etlflow.audit.test)
}
