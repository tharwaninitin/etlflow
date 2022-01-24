package etlflow.model

sealed abstract class EtlFlowException(msg: String) extends RuntimeException(msg)

object EtlFlowException {

  case class DBException(msg: String) extends EtlFlowException(msg) {
    override def toString: String = s"$msg"
  }

  case class EtlJobException(msg: String) extends EtlFlowException(msg) {
    override def toString: String = s"$msg"
  }

  case class RetryException(msg: String) extends EtlFlowException(msg) {
    override def toString: String = s"$msg"
  }

  case class EtlJobNotFoundException(msg: String) extends EtlFlowException(msg) {
    override def toString: String = s"$msg"
  }
}
