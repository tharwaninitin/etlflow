package etlflow.log

import org.slf4j.{Logger, LoggerFactory}
import zio.{Runtime, ULayer}
import zio.logging.backend.SLF4J

trait ApplicationLogger {

  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  protected val zioSlf4jLogger: ULayer[Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j
}
