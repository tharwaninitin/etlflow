package etlflow.json

import etlflow.json.Schema._
import zio.json._

trait Implicits {
  implicit val loggingLevelEncoder: JsonEncoder[LoggingLevel] = JsonEncoder[String].contramap {
    case LoggingLevel.INFO  => "info"
    case LoggingLevel.JOB   => "job"
    case LoggingLevel.DEBUG => "debug"
  }
  implicit val loggingLevelDecoder: JsonDecoder[LoggingLevel] = JsonDecoder[String].map {
    case "info"  => LoggingLevel.INFO
    case "job"   => LoggingLevel.JOB
    case "debug" => LoggingLevel.DEBUG
  }

  implicit val etlJob23PropsEncoder: JsonEncoder[EtlJob23Props] = DeriveJsonEncoder.gen
  implicit val etlJob23PropsDecoder: JsonDecoder[EtlJob23Props] = DeriveJsonDecoder.gen
  implicit val studentDecoder: JsonDecoder[Student]             = DeriveJsonDecoder.gen
}
