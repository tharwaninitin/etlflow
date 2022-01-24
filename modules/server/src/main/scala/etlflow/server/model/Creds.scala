package etlflow.server.model

sealed trait Creds
object Creds {
  case object AWS  extends Creds
  case object JDBC extends Creds
}
