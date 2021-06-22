package etlflow.utils

sealed trait HttpMethod
object HttpMethod {
  case object GET extends HttpMethod
  case object POST extends HttpMethod
  case object PUT extends HttpMethod
}
