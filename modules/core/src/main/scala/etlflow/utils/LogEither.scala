package etlflow.utils

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object LogEither extends ApplicationLogger {
  def apply[E <: Throwable: ClassTag, A](computation: => A): Either[E, A] = Try(computation) match {
    case Failure(throwable) =>
      throwable match {
        case exception: E =>
          logger.error(s"Failure: $exception")
          Left(exception)
        case _ =>
          logger.error(s"Defect: $throwable")
          throw throwable
      }
    case Success(value) => Right(value)
  }
}
