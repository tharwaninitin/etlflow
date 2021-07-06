package json

import io.circe.generic.semiauto.deriveDecoder
import io.circe.Decoder
import io.circe.Encoder

object Schema {

  case class Student(id: String, name: String, `class`: Option[String])

  object Student {
    given studentDecoder: Decoder[Student] = deriveDecoder[Student]
  }
}
