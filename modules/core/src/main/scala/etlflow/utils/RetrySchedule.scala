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

  /** @param retry
    *   Number of times effect needs to run before it terminates
    * @param spaced
    *   Specifies duration each repetition should be spaced from the last run
    * @return
    *   Schedule
    */
  def recurs(retry: Int, spaced: Duration): Schedule[Any, Throwable, (Throwable, Long, Long)] =
    (noThrowable && Schedule.recurs(retry) && Schedule.spaced(ZDuration.fromScala(spaced))).onDecision {
      case (_, out, Decision.Done) => ZIO.logError(s"Exception occurred => ${out._1}")
      case (_, out, Decision.Continue(_)) =>
        ZIO.logInfo(s"Retry condition encountered => ${out._1}") *> ZIO.logInfo(s"Retrying attempt #${out._2 + 1}")
    }

  /** @param spaced
    *   Specifies duration each repetition should be spaced from the last run
    * @return
    *   Schedule
    */
  def forever(spaced: Duration): Schedule[Any, Throwable, (Throwable, Long, Long)] =
    (noThrowable && Schedule.forever && Schedule.spaced(ZDuration.fromScala(spaced))).onDecision {
      case (_, out, Decision.Done) => ZIO.logError(s"Exception occurred => ${out._1}")
      case (_, out, Decision.Continue(_)) =>
        ZIO.logInfo(s"Retry condition encountered => ${out._1}") *> ZIO.logInfo(s"Retrying attempt #${out._2 + 1}")
    }
}
