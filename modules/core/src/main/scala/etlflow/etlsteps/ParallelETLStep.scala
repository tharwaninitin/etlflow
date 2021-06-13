package etlflow.etlsteps

import etlflow.JobEnv
import etlflow.utils.{Configuration, LoggingLevel}
import zio.{RIO, ZIO, ZLayer}
import etlflow.log.DbStepLogger.StepReq

case class ParallelETLStep(name: String)(steps: EtlStep[Unit,Unit]*) extends EtlStep[Unit,Unit] with Configuration {

  val job_run_id: String = java.util.UUID.randomUUID.toString

  final def process(in: => Unit): RIO[JobEnv, Unit] = {
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting steps => ${steps.map(_.name).mkString(",")} in parallel")
    val stepLayer = ZLayer.succeed(StepReq(job_run_id))
    ZIO.foreachPar(steps)(x => x.execute()).provideSomeLayer[JobEnv](stepLayer).unit
  }

  final override def getStepProperties(level: LoggingLevel): Map[String, String] =
    Map("parallel_steps" -> steps.map(_.name).mkString(","), "step_run_id" -> job_run_id)
}


