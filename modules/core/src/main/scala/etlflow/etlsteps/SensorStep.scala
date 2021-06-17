package etlflow.etlsteps

import etlflow.common.EtlflowError.EtlJobException
import org.slf4j.{Logger, LoggerFactory}
import zio.Schedule.Decision
import zio.clock.Clock
import zio.duration.{Duration => ZDuration}
import zio.{Schedule, Task}

import scala.concurrent.duration.Duration

trait SensorStep {
  val sensor_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  lazy val noThrowable: Schedule[Any, Throwable, Throwable] = Schedule.recurWhile {
    case _: EtlJobException => true
    case _ => false
  }

  def schedule[A](retry: Int, spaced: Duration): Schedule[Clock, A, (Long, Long)] =
    (Schedule.recurs(retry) && Schedule.spaced(ZDuration.fromScala(spaced))).onDecision {
      case Decision.Done(_)             => Task.succeed(sensor_logger.info(s"done trying"))
      case Decision.Continue(att, _, _) => Task.succeed(sensor_logger.info(s"retry #$att"))
    }
}
