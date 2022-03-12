package etlflow.http

sealed trait HttpMethod {
  override def toString: String = this.getClass.getSimpleName
}
object HttpMethod {
  case object GET  extends HttpMethod
  case object POST extends HttpMethod
  case object PUT  extends HttpMethod
}
