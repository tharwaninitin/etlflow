package etljobs.utils

sealed trait IOType extends Serializable

final case class CSV(delimiter : String = ",", header_present : Boolean = true, parse_mode : String = "FAILFAST") extends IOType {
  override def toString: String = s"CSV with delimiter => $delimiter header_present => $header_present parse_mode => $parse_mode"
}
final case class MCSV(delimiter : String, no_of_columns : Int) extends IOType
final case class JDBC(url: String, user: String, password: String, driver: String) extends IOType {
  override def toString: String = s"JDBC with url => $url"
}
final case class JSON(multi_line : Boolean = false) extends IOType {
  override def toString: String = s"Json with multiline  => $multi_line"
}
case object BQ extends IOType
case object PARQUET extends IOType
case object ORC extends IOType
case object TEXT extends IOType
case object EXCEL extends IOType