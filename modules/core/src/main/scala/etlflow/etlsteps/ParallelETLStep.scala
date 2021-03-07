package etlflow.etlsteps

import etlflow.utils.LoggingLevel
import zio.{Task, ZIO}

case class ParallelETLStep(name: String)(steps: EtlStep[Unit,Unit]*) extends EtlStep[Unit,Unit] {
  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting steps => ${steps.map(_.name).mkString(",")} in parallel")
    ZIO.collectAllPar(steps.map(x => x.process())).as(())
  }
  final override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("parallel_steps" -> steps.map(_.name).mkString(","))
}


