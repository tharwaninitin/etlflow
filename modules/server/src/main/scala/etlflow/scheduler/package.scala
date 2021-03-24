package etlflow

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import cron4s.expr.CronExpr
import cron4s.lib.javatime._
import zio.clock.{Clock, sleep}
import zio.duration.Duration
import zio.{RIO, Schedule, Task, UIO, ZIO}

package object scheduler {

  def sleepForCron(cronExpr: CronExpr): ZIO[Clock, Throwable, Unit] =
    getNextDuration(cronExpr).flatMap(duration => {
      sleep(duration)
    })

  def getNextDuration(cronExpr: CronExpr): Task[Duration] = {
    for {
      timeNow           <- ZIO.effectTotal(LocalDateTime.now)
      timeNext          <- ZIO.fromOption(cronExpr.next(timeNow)).mapError(_ => new Throwable("Non Recoverable Error"))
      durationInNanos   = timeNow.until(timeNext, ChronoUnit.NANOS)
      duration          = Duration.fromNanos(durationInNanos)
    } yield duration
  }

  def repeatEffectForCron[A](effect: UIO[A], cronExpr: CronExpr, maxRecurs: Int = 0): RIO[Clock, Long] =
    if (maxRecurs != 0)
      (sleepForCron(cronExpr) *> effect).repeat(Schedule.recurs(maxRecurs))
    else
      (sleepForCron(cronExpr) *> effect).repeat(Schedule.forever)

  def repeatEffectsForCron[A](tasks: List[(CronExpr,UIO[A])]): RIO[Clock, Unit] = {
    val scheduled = tasks.map { case (cronExpr, task) => (sleepForCron(cronExpr) *> task).repeat(Schedule.forever) }
    ZIO.collectAllPar(scheduled).as(())
  }
}
