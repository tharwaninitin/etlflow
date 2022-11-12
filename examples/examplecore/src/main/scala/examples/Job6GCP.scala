package examples

import etlflow.task._
import zio._

object Job6GCP extends ZIOAppDefault {
  override def run: Task[Unit] = GenericTask("Hello", println("Hello World")).execute.provide(etlflow.audit.test)
}
