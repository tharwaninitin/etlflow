package etlflow.etlsteps

import etlflow.CoreEnv
import etlflow.crypto.CryptoEnv
import etlflow.db.DBEnv
import etlflow.json.JsonEnv
import etlflow.log.LoggerApi
import etlflow.schema.LoggingLevel
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{RIO, ZIO, ZLayer}

case class ParallelETLStep(name: String)(steps: EtlStep[Unit,Unit]*) extends EtlStep[Unit,Unit] {

  var job_run_id: String = java.util.UUID.randomUUID.toString

  final def process(in: => Unit): RIO[CoreEnv, Unit] = {
    logger.info("#################################################################################################")
    logger.info(s"Starting steps => ${steps.map(_.name).mkString(",")} in parallel")
    (for{
      _   <- LoggerApi.setJobRunId(job_run_id)
      _   <- ZIO.foreachPar_(steps)(x => x.execute())
    } yield ()).provideSomeLayer[DBEnv with JsonEnv with CryptoEnv  with  Blocking with Clock](etlflow.log.Implementation.live(None))
  }

  final override def getStepProperties(level: LoggingLevel): Map[String, String] =
    Map("parallel_steps" -> steps.map(_.name).mkString(","), "step_run_id" -> job_run_id)
}


