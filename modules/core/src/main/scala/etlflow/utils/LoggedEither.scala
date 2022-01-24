package etlflow.utils

import scala.reflect.ClassTag

object LoggedEither extends ApplicationLogger {
  def apply[E <: Throwable, A](computation: => A)(implicit ct: ClassTag[E]): Either[E, A] =
    try Right(computation)
    catch {
      case e: E =>
        logger.error(s"Failure: $e")
        Left(e)
      case e: Throwable =>
        logger.error(s"Defect: expected error of ${ct.runtimeClass.toString} but found error $e")
        throw e
    }
}
