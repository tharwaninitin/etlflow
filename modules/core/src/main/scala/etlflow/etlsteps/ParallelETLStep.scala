package etlflow.etlsteps

import zio.{Task, ZIO}

case class ParallelETLStep(steps: EtlStep[Unit,Unit]*) extends EtlStep[Unit,Unit] {
  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting steps => ${steps.map(_.name).mkString(",")} in parallel")
    ZIO.collectAllPar(steps.map(x => x.process())).as(())
  }
  final override def getStepProperties(level: String): Map[String, String] = Map("parallel_steps" -> steps.map(_.name).mkString(","))
  final override val name: String = s"ParallelStepWith${steps.map(_.name).size}Steps"
}


