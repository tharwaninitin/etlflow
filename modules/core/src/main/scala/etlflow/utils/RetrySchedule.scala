package etlflow.utils

import etlflow.model.EtlFlowException.RetryException
import zio.Schedule.Decision
import zio.{Duration => ZDuration, Schedule, ZIO}
import scala.concurrent.duration.Duration

object RetrySchedule {

  private val noThrowable: Schedule[Any, Throwable, Throwable] = Schedule.recurWhile[Throwable] {
    case _: RetryException => true
    case _                 => false
  }

  def recurs[A](retry: Int, spaced: Duration): Schedule[Any, Throwable, (Throwable, Long, Long)] =
    (noThrowable && Schedule.recurs(retry) && Schedule.spaced(ZDuration.fromScala(spaced))).onDecision {
      case (_, out, Decision.Done) => ZIO.logError(s"Exception occurred => ${out._1}")
      case (_, out, Decision.Continue(_)) =>
        ZIO.logError(s"Exception occurred => ${out._1}") *> ZIO.logError(s"Retrying attempt #${out._2 + 1}")
    }

  def forever[A](spaced: Duration): Schedule[Any, Throwable, (Throwable, Long, Long)] =
    (noThrowable && Schedule.forever && Schedule.spaced(ZDuration.fromScala(spaced))).onDecision {
      case (_, out, Decision.Done) => ZIO.logError(s"Exception occurred => ${out._1}")
      case (_, out, Decision.Continue(_)) =>
        ZIO.logError(s"Exception occurred => ${out._1}") *> ZIO.logError(s"Retrying attempt #${out._2 + 1}")
    }
}
