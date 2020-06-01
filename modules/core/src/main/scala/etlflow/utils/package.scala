package etlflow

package object utils {
  sealed trait FSType
  case object LOCAL extends FSType
  case object GCS extends FSType

  final case class GCP(service_account_key_path: String) {
    override def toString: String = "****service_account_key_path****"
  }
  final case class AWS(access_key: String, secret_key: String) {
    override def toString: String = "****access_key****secret_key****"
  }

  sealed trait IOType extends Serializable
  final case class CSV(delimiter: String = ",", header_present: Boolean = true, parse_mode: String = "FAILFAST", quotechar: String = "\"") extends IOType {
    override def toString: String = s"CSV with delimiter => $delimiter header_present => $header_present parse_mode => $parse_mode"
  }
  final case class MCSV(delimiter: String, no_of_columns: Int) extends IOType
  final case class JDBC(url: String, user: String, password: String, driver: String) extends IOType {
    override def toString: String = s"JDBC with url => $url"
  }
  final case class REDIS(url: String, user: String, password: String, port: String) extends IOType {
    override def toString: String = s"REDIS with url $url and port $port"
  }
  final case class JSON(multi_line: Boolean = false) extends IOType {
    override def toString: String = s"Json with multiline  => $multi_line"
  }
  case object BQ extends IOType
  case object PARQUET extends IOType
  case object ORC extends IOType
  case object TEXT extends IOType
  case object EXCEL extends IOType
}