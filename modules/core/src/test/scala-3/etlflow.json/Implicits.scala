package etlflow.json

import etlflow.json.Schema._
import zio.json._

trait Implicits {
  given JsonEncoder[LoggingLevel] = JsonEncoder[String].contramap {
    case LoggingLevel.INFO  => "info"
    case LoggingLevel.JOB   => "job"
    case LoggingLevel.DEBUG => "debug"
  }
  given JsonDecoder[LoggingLevel] = JsonDecoder[String].map {
    case "info"  => LoggingLevel.INFO
    case "job"   => LoggingLevel.JOB
    case "debug" => LoggingLevel.DEBUG
  }

  given JsonDecoder[Student] = DeriveJsonDecoder.gen
  given JsonEncoder[EtlJob23Props] = DeriveJsonEncoder.gen
  given JsonDecoder[EtlJob23Props] = DeriveJsonDecoder.gen
}
