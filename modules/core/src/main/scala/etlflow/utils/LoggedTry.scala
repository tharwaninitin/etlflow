package etlflow.utils

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

// https://stackoverflow.com/questions/24394291/try-with-exception-logging
// https://stackoverflow.com/questions/24007947/analogous-try-block-to-try-finally-block-in-scala

object LoggedTry extends ApplicationLogger {
  def apply[A](
      computation: => A,
      failure: Throwable => Unit = e => logger.error(s"Failure: $e"),
      defect: Throwable => Unit = e => logger.error(s"Defect: $e")
  ): Try[A] =
    try Success(computation)
    catch {
      case NonFatal(e) =>
        failure(e)
        Failure(e)
      case e: Throwable =>
        defect(e)
        throw e
    }
}
