package etlflow

package schema {
  case class Student(id: String, name: String, `class`: Option[String])
  case class HttpBinResponse(args: Map[String, String], headers: Map[String, String], origin: String, url: String)
}
