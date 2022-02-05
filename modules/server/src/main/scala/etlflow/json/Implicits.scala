package etlflow.json

import etlflow.model.Credential._
import zio.json._

object Implicits {
  implicit val jdbc_decoder: JsonDecoder[JDBC] = DeriveJsonDecoder.gen[JDBC]
  implicit val jdbc_encoder: JsonEncoder[JDBC] = DeriveJsonEncoder.gen[JDBC]
  implicit val aws_decoder: JsonDecoder[AWS]   = DeriveJsonDecoder.gen[AWS]
  implicit val aws_encoder: JsonEncoder[AWS]   = DeriveJsonEncoder.gen[AWS]
}
