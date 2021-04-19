package etlflow.etlsteps

import etlflow.log.DbStepLogger
import etlflow.utils.{Configuration, LoggingLevel}
import etlflow.{JobEnv, StepLogger}
import zio.{RIO, ZIO, ZLayer}

case class ParallelETLStep(name: String)(steps: EtlStep[Unit,Unit]*) extends EtlStep[Unit,Unit] with Configuration {

  val job_run_id: String = java.util.UUID.randomUUID.toString

  final def process(in: => Unit): RIO[JobEnv, Unit] = {
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting steps => ${steps.map(_.name).mkString(",")} in parallel")
    for {
      db      <- DbStepLogger(job_run_id, job_enable_db_logging = true, LoggingLevel.INFO)
      layer   = ZLayer.succeed(StepLogger(db,None))
      _       <- ZIO.foreachPar(steps)(x => x.execute()).provideSomeLayer[JobEnv](layer)
    } yield ()
  }

  final override def getStepProperties(level: LoggingLevel): Map[String, String] =
    Map("parallel_steps" -> steps.map(_.name).mkString(","), "step_run_id" -> job_run_id)
}


