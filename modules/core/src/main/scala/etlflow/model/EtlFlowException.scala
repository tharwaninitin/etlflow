package etlflow.model

sealed abstract class EtlFlowException(msg: String) extends RuntimeException(msg)

object EtlFlowException {

  final case class EtlJobException(msg: String) extends EtlFlowException(msg) {
    override def toString: String = s"$msg"
  }

  final case class RetryException(msg: String) extends EtlFlowException(msg) {
    override def toString: String = s"$msg"
  }

  final case class JsonDecodeException(msg: String) extends EtlFlowException(msg) {
    override def toString: String = s"$msg"
  }
}
