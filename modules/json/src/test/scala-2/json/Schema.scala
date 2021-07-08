package json

import io.circe.generic.semiauto.deriveDecoder

object Schema {

  case class Student(id: String, name: String, `class`: Option[String])

  object Student {
    implicit val studentDecoder = deriveDecoder[Student]
  }
}
