package etlflow.utils

import scala.util.{Failure, Try}

// https://stackoverflow.com/questions/24394291/try-with-exception-logging
// https://stackoverflow.com/questions/24007947/analogous-try-block-to-try-finally-block-in-scala

object LogTry extends ApplicationLogger {
  def apply[A](computation: => A): Try[A] = Try(computation) match {
    case failure @ Failure(throwable) =>
      throwable match {
        case exception: Exception =>
          logger.error(s"Failure: $exception")
          failure
        case _ =>
          logger.error(s"Defect: $throwable")
          throw throwable
      }
    case success @ _ => success
  }
}
