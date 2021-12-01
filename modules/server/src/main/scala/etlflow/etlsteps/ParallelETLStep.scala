package etlflow.etlsteps

import etlflow.core.CoreEnv
import etlflow.log.LoggerApi
import etlflow.schema.LoggingLevel
import etlflow.utils.Configuration
import zio.{RIO, ZIO}

case class ParallelETLStep(name: String)(steps: EtlStep[Unit,Unit]*) extends EtlStep[Unit,Unit] {

  var job_run_id: String = java.util.UUID.randomUUID.toString

  final def process(in: => Unit): RIO[CoreEnv, Unit] = {
    logger.info("#################################################################################################")
    logger.info(s"Starting steps => ${steps.map(_.name).mkString(",")} in parallel")
    val layer = etlflow.log.Implementation.live ++ etlflow.log.SlackImplementation.nolog ++ etlflow.log.ConsoleImplementation.live
    for{
      cfg <- Configuration.config
      _   <- (LoggerApi.setJobRunId(job_run_id) *> ZIO.foreachPar_(steps)(x => x.execute(()))).provideSomeLayer[CoreEnv](layer ++ etlflow.log.DBLiveImplementation(cfg.db.get, "Parallel-Step-" + name + "-Pool", 1))
    } yield ()
  }

  final override def getStepProperties(level: LoggingLevel): Map[String, String] =
    Map("parallel_steps" -> steps.map(_.name).mkString(","), "step_run_id" -> job_run_id)
}


