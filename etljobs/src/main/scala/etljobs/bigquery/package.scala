package etljobs

package object bigquery {
  case class BQLoadException(msg : String) extends Exception
}
