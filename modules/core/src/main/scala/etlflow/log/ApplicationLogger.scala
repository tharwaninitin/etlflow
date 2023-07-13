package etlflow.log

import org.slf4j.{Logger, LoggerFactory}
import zio.{Runtime, ULayer}
import zio.logging.backend.SLF4J

trait ApplicationLogger {

  /**
   * The logger instance used for logging within the application.
   */
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * The ZIO logging layer for SLF4J backend.
   */
  protected val zioSlf4jLogger: ULayer[Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j
}