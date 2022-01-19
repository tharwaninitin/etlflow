package etlflow.utils

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

// https://stackoverflow.com/questions/24394291/try-with-exception-logging
// https://stackoverflow.com/questions/24007947/analogous-try-block-to-try-finally-block-in-scala

object LoggedTry extends ApplicationLogger {
  def apply[A](computation: => A): Try[A] =
    try Success(computation)
    catch {
      case NonFatal(e) =>
        logger.error(s"Failure: $e")
        Failure(e)
      case e: Throwable =>
        logger.error(s"Defect: $e")
        throw e
    }
}
