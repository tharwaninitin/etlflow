package etlflow.log

import org.slf4j.{Logger, LoggerFactory}

private[etlflow] trait ApplicationLogger {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}
