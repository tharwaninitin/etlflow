package etlflow

import zio.{UIO, ULayer, URIO, ZIO, ZLayer}

package object audit {
  // format: off
  trait Audit {
    val jobRunId: String
    
    def logJobStart(jobName: String, args: Map[String,String], props: Map[String,String], startTime: Long): UIO[Unit]
    def logJobEnd(jobName: String, args: Map[String,String], props: Map[String,String], endTime: Long, error: Option[Throwable]): UIO[Unit]
    
    def logTaskStart(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, startTime: Long): UIO[Unit]
    def logTaskEnd(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, endTime: Long, error: Option[Throwable]): UIO[Unit]
  }

  object Audit {
    def logJobStart(jobName: String, args: Map[String,String], props: Map[String,String], startTime: Long): URIO[Audit, Unit] =
      ZIO.environmentWithZIO(_.get.logJobStart(jobName, args, props, startTime))
    def logJobEnd(jobName: String, args: Map[String,String], props: Map[String,String], endTime: Long, error: Option[Throwable] = None): URIO[Audit, Unit] =
      ZIO.environmentWithZIO(_.get.logJobEnd(jobName, args, props, endTime, error))
    
    def logTaskStart(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, startTime: Long): URIO[Audit, Unit] =
      ZIO.environmentWithZIO(_.get.logTaskStart(taskRunId, taskName, props, taskType, startTime))
    def logTaskEnd(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, endTime: Long, error: Option[Throwable] = None): URIO[Audit, Unit] =
      ZIO.environmentWithZIO(_.get.logTaskEnd(taskRunId, taskName, props, taskType, endTime, error))
  }

  val test: ULayer[Audit] = ZLayer.succeed(
    new Audit {
      override val jobRunId: String = ""
      
      override def logJobStart(jobName: String, args: Map[String,String], props: Map[String,String], startTime: Long): UIO[Unit] = ZIO.unit
      override def logJobEnd(jobName: String, args: Map[String,String], props: Map[String,String], endTime: Long, error: Option[Throwable]): UIO[Unit] = ZIO.unit
      
      override def logTaskStart(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, startTime: Long): UIO[Unit] = ZIO.unit
      override def logTaskEnd(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, endTime: Long, error: Option[Throwable]): UIO[Unit] = ZIO.unit
    }
  )

  val console: ULayer[Audit] = ZLayer.succeed(Console)
  def memory(jobRunId: String): ULayer[Audit] = ZLayer.succeed(Memory(jobRunId))
  def slack(jri: String, slackUrl: String): ULayer[Audit] = ZLayer.succeed(Slack(jri, slackUrl))
  // format: on
}
