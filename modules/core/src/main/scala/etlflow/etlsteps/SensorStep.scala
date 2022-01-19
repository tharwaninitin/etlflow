package etlflow.etlsteps

import etlflow.utils.ApplicationLogger
import etlflow.model.EtlflowError.EtlJobException
import zio.Schedule.Decision
import zio.clock.Clock
import zio.duration.{Duration => ZDuration}
import zio.{Schedule, Task}
import scala.concurrent.duration.Duration

trait SensorStep extends ApplicationLogger {

  lazy val noThrowable: Schedule[Any, Throwable, Throwable] = Schedule.recurWhile {
    case _: EtlJobException => true
    case _                  => false
  }

  def schedule[A](retry: Int, spaced: Duration): Schedule[Clock, A, (Long, Long)] =
    (Schedule.recurs(retry) && Schedule.spaced(ZDuration.fromScala(spaced))).onDecision {
      case Decision.Done(_)             => Task.succeed(logger.info(s"done trying"))
      case Decision.Continue(att, _, _) => Task.succeed(logger.info(s"retry #$att"))
    }
}
