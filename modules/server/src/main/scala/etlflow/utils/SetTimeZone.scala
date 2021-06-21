package etlflow.utils

import java.util.TimeZone
import java.util.Calendar
import etlflow.log.ApplicationLogger
import etlflow.schema.Config
import zio.Task

private [etlflow]  object SetTimeZone extends ApplicationLogger {
  def apply(config: Config): Task[Unit] = Task {
    config.timezone.foreach{tz =>
      TimeZone.setDefault(TimeZone.getTimeZone(tz))
      logger.info(s"TimeZone provided in application.conf $tz")
    }
    logger.info(s"TimeZone set to ${Calendar.getInstance.getTimeZone.getID}")
  }
}
