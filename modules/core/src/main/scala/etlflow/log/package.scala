package etlflow

import zio.{UIO, ULayer, URIO, ZIO, ZLayer}
import scala.util.Try

package object log {
  type LogEnv    = Service[UIO]
  type LogEnvTry = Service[Try]
  
  // format: off
  trait Service[F[_]] {
    val jobRunId: String
    def logTaskStart(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, startTime: Long): F[Unit]
    def logTaskEnd(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, endTime: Long, error: Option[Throwable]): F[Unit]
    def logJobStart(jobName: String, args: String, startTime: Long): F[Unit]
    def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable]): F[Unit]
  }

  object LogApi {
    def logTaskStart(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, startTime: Long): URIO[LogEnv, Unit] =
      ZIO.environmentWithZIO(_.get.logTaskStart(taskRunId, taskName, props, taskType, startTime))
    def logTaskEnd(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, endTime: Long, error: Option[Throwable] = None): URIO[LogEnv, Unit] =
      ZIO.environmentWithZIO(_.get.logTaskEnd(taskRunId, taskName, props, taskType, endTime, error))
    def logJobStart(jobName: String, args: String, startTime: Long): URIO[LogEnv, Unit] =
      ZIO.environmentWithZIO(_.get.logJobStart(jobName, args, startTime))
    def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable] = None): URIO[LogEnv, Unit] =
      ZIO.environmentWithZIO(_.get.logJobEnd(jobName, args, endTime, error))
  }

  val noLog: ULayer[LogEnv] = ZLayer.succeed(
    new Service[UIO] {
      override val jobRunId: String = ""
      override def logTaskStart(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, startTime: Long): UIO[Unit] = ZIO.unit
      override def logTaskEnd(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, endTime: Long, error: Option[Throwable]): UIO[Unit] = ZIO.unit
      override def logJobStart(jobName: String, args: String, startTime: Long): UIO[Unit] = ZIO.unit
      override def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable]): UIO[Unit] = ZIO.unit
    }
  )
  
  val noLogTry: LogEnvTry = new LogEnvTry {
      override val jobRunId: String = ""
      override def logTaskStart(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, startTime: Long): Try[Unit] = Try(())
      override def logTaskEnd(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, endTime: Long, error: Option[Throwable]): Try[Unit] = Try(())
      override def logJobStart(jobName: String, args: String, startTime: Long): Try[Unit] = Try(())
      override def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable]): Try[Unit] = Try(())
    }
  // format: on
}
