package etljobs.utils

sealed trait IOType

final case class CSV(delimiter : String = ",", header_present : Boolean = true, parse_mode : String = "FAILFAST") extends IOType {
  override def toString: String = s"CSV with delimiter => $delimiter header_present => $header_present parse_mode => $parse_mode"
}
final case class MCSV(delimiter : String, no_of_columns : Int) extends IOType
final case class JDBC(url: String, user: String, password: String) extends IOType {
  override def toString: String = s"JDBC with url => $url"
}
case object BQ extends IOType
case object PARQUET extends IOType
case object ORC extends IOType
case object TEXT extends IOType
case object EXCEL extends IOType
case object JSON extends IOType
