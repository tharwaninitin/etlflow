package etlflow.db

import doobie.util.log.{ExecFailure, LogHandler, ProcessingFailure, Success}
import org.slf4j.{Logger, LoggerFactory}

private[db] object DoobieQueryLogger {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def apply(): LogHandler = {
    LogHandler {
      case Success(s, a, e1, e2) =>
        logger.info(s"${s.linesIterator.dropWhile(_.trim.isEmpty).mkString("\n")}")
        logger.info(s"arguments = [${a.mkString(", ")}]")
        logger.info(s"elapsed = ${e1.toMillis.toString} ms exec + ${e2.toMillis.toString} ms processing (${(e1 + e2).toMillis.toString} ms total)")
      case ProcessingFailure(s, a, e1, e2, t) =>
        logger.error(s"${s.linesIterator.dropWhile(_.trim.isEmpty).mkString("  ")}")
        logger.error(s"arguments = [${a.mkString(", ")}]")
        logger.error(s"elapsed = ${e1.toMillis.toString} ms exec + ${e2.toMillis.toString} ms processing (failed) (${(e1 + e2).toMillis.toString} ms total)")
        logger.error(s"failure = ${t.getMessage}")
      case ExecFailure(s, a, e1, t) =>
        logger.error(s"${s.linesIterator.dropWhile(_.trim.isEmpty).mkString("  ")}")
        logger.error(s"arguments = [${a.mkString(", ")}]")
        logger.error(s"elapsed = ${e1.toMillis.toString} ms exec (failed)")
        logger.error(s"failure = ${t.getMessage}")
    }
  }
}
