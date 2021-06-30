package etlflow.utils

sealed abstract class EtlflowError(msg : String) extends RuntimeException(msg)

object EtlflowError {

  case class DBException(msg : String) extends EtlflowError(msg) {
    override def toString: String    = s"$msg"
  }

  case class EtlJobException(msg : String) extends EtlflowError(msg) {
    override def toString: String    = s"$msg"
  }

  case class EtlJobNotFoundException(msg : String) extends EtlflowError(msg) {
    override def toString: String    = s"$msg"
  }
}
