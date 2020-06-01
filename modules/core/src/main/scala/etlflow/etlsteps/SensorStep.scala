package etlflow.etlsteps

import etlflow.EtlJobException
import org.slf4j.{Logger, LoggerFactory}
import zio.{Schedule, Task}
import zio.clock.Clock
import zio.duration.{Duration => ZDuration}
import scala.concurrent.duration.Duration

trait SensorStep {
  val sensor_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  lazy val noThrowable: Schedule[Any, Throwable, Throwable] = Schedule.doWhile {
    case _: EtlJobException => true
    case _ => false
  }

  def schedule[A](retry: Int, spaced: Duration): Schedule[Clock, A, (Int, Int)] =
    Schedule.recurs(retry) && Schedule.spaced(ZDuration.fromScala(spaced)).onDecision((a: A, s) => s match {
      case None => Task.succeed(sensor_logger.info(s"done trying"))
      case Some(att) => Task.succeed(sensor_logger.info(s"retry #$att"))
    })
}
