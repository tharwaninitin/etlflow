package etlflow.etlsteps

import cats.effect.Blocker
import etlflow.log.DbStepLogger
import etlflow.utils.{Configuration}
import etlflow.StepLogger
import zio.blocking.Blocking
import zio.internal.Platform
import zio.{Task, ZEnv, ZIO, ZLayer}
import etlflow.utils.LoggingLevel

case class ParallelETLStep(name: String)(steps: EtlStep[Unit,Unit]*) extends EtlStep[Unit,Unit] with Configuration {

  val job_run_id: String = java.util.UUID.randomUUID.toString

  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting steps => ${steps.map(_.name).mkString(",")} in parallel")
    (for {
      blocker <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
      db      <- DbStepLogger(config, Platform.default.executor.asEC, blocker, "Parallel-Step-Pool", "Parallel-Step", job_run_id, "false",job_notification_level = LoggingLevel.INFO,job_enable_db_logging = true)
      layer   = ZLayer.succeed(StepLogger(db,None))
      _       <- ZIO.collectAllPar(steps.map(x => x.execute())).provideCustomLayer(layer).toManaged_
    } yield ()).use_(ZIO.unit).provideLayer(ZEnv.live)
  }

  final override def getStepProperties(level: LoggingLevel): Map[String, String] =
    Map("parallel_steps" -> steps.map(_.name).mkString(","),"step_run_id" -> job_run_id)
}


