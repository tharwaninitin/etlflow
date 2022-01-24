package etlflow.utils

import etlflow.model.EtlFlowException.RetryException
import zio.Schedule.Decision
import zio.clock.Clock
import zio.{Schedule, UIO}
import scala.concurrent.duration.Duration
import zio.duration.{Duration => ZDuration}

object RetrySchedule extends ApplicationLogger {

  private val noThrowable: Schedule[Any, Throwable, Throwable] = Schedule.recurWhile {
    case _: RetryException => true
    case _                 => false
  }

  def apply[A](retry: Int, spaced: Duration): Schedule[Clock, Throwable, ((Throwable, Long), Long)] =
    (noThrowable && Schedule.recurs(retry) && Schedule.spaced(ZDuration.fromScala(spaced))).onDecision {
      case Decision.Done(out) => UIO(logger.error(s"Exception occurred => ${out._1._1}"))
      case Decision.Continue(out, _, _) =>
        UIO {
          logger.error(s"Exception occurred => ${out._1._1}")
          logger.error(s"Retrying attempt #${out._1._2 + 1}")
        }
    }
}
