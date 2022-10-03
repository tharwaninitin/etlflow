package etlflow.utils

import etlflow.model.EtlFlowException.RetryException
import zio.Schedule.Decision
import zio.{Clock, Duration => ZDuration, Schedule, ZIO}
import scala.concurrent.duration.Duration

object RetrySchedule extends ApplicationLogger {

  private val noThrowable: Schedule[Any, Throwable, Throwable] = Schedule.recurWhile[Throwable] {
    case _: RetryException => true
    case _                 => false
  }

  def apply[A](retry: Int, spaced: Duration): Schedule[Clock, Throwable, (Throwable, Long, Long)] =
    (noThrowable && Schedule.recurs(retry) && Schedule.spaced(ZDuration.fromScala(spaced))).onDecision {
      case (_, out, Decision.Done) => ZIO.succeed(logger.error(s"Exception occurred => ${out._1}"))
      case (_, out, Decision.Continue(_)) =>
        ZIO.succeed {
          logger.error(s"Exception occurred => ${out._1}")
          logger.error(s"Retrying attempt #${out._2 + 1}")
        }
    }
}
