package etlflow

import com.cronutils.model.definition.{CronConstraintsFactory, CronDefinitionBuilder}
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import zio.clock.{Clock, sleep}
import zio.duration.Duration
import zio.{RIO, Schedule, Task, UIO, URIO, ZIO}

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.time.temporal.ChronoUnit
import java.util.TimeZone
import scala.util.Try

package object scheduler {


 /* Our cron definition uses cron expressions that go from seconds to day of week in the following order:

    Seconds	0-59	                    - * /
    Minutes	0-59	                    - * /
    Hours	0-23	                      - * /
    Day (of month)	1-31	            * ? / L W
    Month	1-12 or JAN-DEC	            - * /
    Day (of week)	1-7 or SUN-SAT	    - * ? / L #
    Year (optional)	empty, 1970-2099	- * /
  */

  val  initiateCron =
    CronDefinitionBuilder.defineCron()
      .withSeconds().withValidRange(0, 59).and()
      .withMinutes().withValidRange(0, 59).and()
      .withHours().withValidRange(0, 23).and()
      .withDayOfMonth().withValidRange(1, 31).supportsL().supportsW().supportsLW().supportsQuestionMark().and()
      .withMonth().withValidRange(1, 12).and()
      .withDayOfWeek().withValidRange(1, 7).withMondayDoWValue(0).supportsHash().supportsL().supportsQuestionMark().and()
      .withYear().withValidRange(1970, 2099).withStrictRange().optional().and()
      .withCronValidation(CronConstraintsFactory.ensureEitherDayOfWeekOrDayOfMonth())
      .instance()

  val zoneId: ZoneId = TimeZone.getDefault.toZoneId

  def parseCron(cron: String): Option[ExecutionTime] = {
    parseCronTry(cron).toOption
  }

  def parseCronTry(cron: String): Try[ExecutionTime] = {
    Try(ExecutionTime.forCron(new CronParser(initiateCron)
      .parse(cron)))
  }

  def sleepForCron(cronExpr: ExecutionTime): RIO[Clock, Unit] =
    getNextDuration(cronExpr).flatMap(duration => {
      sleep(duration)
    })

  def getNextDuration(cronExpr: ExecutionTime): Task[Duration] = {
    for {
      timeNow         <- ZIO.effectTotal(LocalDateTime.now().atZone(zoneId))
      timeNext        <- Task(cronExpr.nextExecution(timeNow).get()).mapError(_ => new Throwable("Non Recoverable Error"))
      durationInNanos =  timeNow.until(timeNext, ChronoUnit.NANOS)
      duration        =  Duration.fromNanos(durationInNanos)
    } yield duration
  }

  def repeatEffectForCron[A](effect: UIO[A], cronExpr: ExecutionTime, maxRecurs: Int = 0): RIO[Clock, Long] =
    if (maxRecurs != 0)
      (sleepForCron(cronExpr) *> effect).repeat(Schedule.recurs(maxRecurs))
    else
      (sleepForCron(cronExpr) *> effect).repeat(Schedule.forever)

  def repeatEffectsForCron[R,A](tasks: List[(ExecutionTime,URIO[R,A])]): RIO[R with Clock, Unit] = {
    val scheduled = tasks.map { case (cronExpr, task) => (sleepForCron(cronExpr) *> task).repeat(Schedule.forever) }
    ZIO.collectAllPar_(scheduled)
  }

  def repeatEffectsForCronWithName[R,A](tasks: List[(String,ExecutionTime,URIO[R,A])]): RIO[R with Clock, Unit] = {
    val scheduled = tasks.map { case (name,cronExpr, task) => (sleepForCron(cronExpr) *> task).repeat(Schedule.forever).forkAs(name) }
    ZIO.collectAllPar_(scheduled)
  }
}
