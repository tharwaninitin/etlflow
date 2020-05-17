package etlflow

package object bigquery {
  case class BQLoadException(msg : String) extends RuntimeException(msg)
}
