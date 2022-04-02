package etlflow

import zio.{Has, UIO, ULayer, URIO, ZIO, ZLayer}

import scala.util.Try

package object log {
  type LogEnv = Has[Service[UIO]]
  // format: off
  trait Service[F[_]] {
    val jobRunId: String
    def logStepStart(stepRunId: String, stepName: String, props: Map[String,String], stepType: String, startTime: Long): F[Unit]
    def logStepEnd(stepRunId: String, stepName: String, props: Map[String,String], stepType: String, endTime: Long, error: Option[Throwable]): F[Unit]
    def logJobStart(jobName: String, args: String, startTime: Long): F[Unit]
    def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable]): F[Unit]
  }

  object LogApi {
    def logStepStart(stepRunId: String, stepName: String, props: Map[String,String], stepType: String, startTime: Long): URIO[LogEnv, Unit] =
      ZIO.accessM(_.get.logStepStart(stepRunId, stepName, props, stepType, startTime))
    def logStepEnd(stepRunId: String, stepName: String, props: Map[String,String], stepType: String, endTime: Long, error: Option[Throwable] = None): URIO[LogEnv, Unit] =
      ZIO.accessM(_.get.logStepEnd(stepRunId, stepName, props, stepType, endTime, error))
    def logJobStart(jobName: String, args: String, startTime: Long): URIO[LogEnv, Unit] =
      ZIO.accessM(_.get.logJobStart(jobName, args, startTime))
    def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable] = None): URIO[LogEnv, Unit] =
      ZIO.accessM(_.get.logJobEnd(jobName, args, endTime, error))
  }

  val noLog: ULayer[LogEnv] = ZLayer.succeed(
    new Service[UIO] {
      override val jobRunId: String = ""
      override def logStepStart(stepRunId: String, stepName: String, props: Map[String,String], stepType: String, startTime: Long): UIO[Unit] = UIO.unit
      override def logStepEnd(stepRunId: String, stepName: String, props: Map[String,String], stepType: String, endTime: Long, error: Option[Throwable]): UIO[Unit] = UIO.unit
      override def logJobStart(jobName: String, args: String, startTime: Long): UIO[Unit] = UIO.unit
      override def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable]): UIO[Unit] = UIO.unit
    }
  )
  
  val noLogTry: Service[Try] = new Service[Try] {
      override val jobRunId: String = ""
      override def logStepStart(stepRunId: String, stepName: String, props: Map[String,String], stepType: String, startTime: Long): Try[Unit] = Try(())
      override def logStepEnd(stepRunId: String, stepName: String, props: Map[String,String], stepType: String, endTime: Long, error: Option[Throwable]): Try[Unit] = Try(())
      override def logJobStart(jobName: String, args: String, startTime: Long): Try[Unit] = Try(())
      override def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable]): Try[Unit] = Try(())
    }
  // format: on
}
