package etlflow

package object schema {
  case class Student(id: String, name: String, `class`: Option[String])
}
