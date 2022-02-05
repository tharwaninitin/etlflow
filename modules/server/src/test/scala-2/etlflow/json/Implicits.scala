package etlflow.json

import etlflow.model.Executor
import etlflow.schema._
import zio.json._

trait Implicits {
  implicit val LoggingLevelEncoder: JsonEncoder[LoggingLevel] = JsonEncoder[String].contramap {
    case LoggingLevel.INFO  => "info"
    case LoggingLevel.JOB   => "job"
    case LoggingLevel.DEBUG => "debug"
  }
  implicit val LoggingLevelDecoder: JsonDecoder[LoggingLevel] = JsonDecoder[String].map {
    case "info"  => LoggingLevel.INFO
    case "job"   => LoggingLevel.JOB
    case "debug" => LoggingLevel.DEBUG
  }
  implicit val ExecutorDecoder: JsonEncoder[Executor] = JsonEncoder[String].contramap {
    case Executor.DATAPROC(_, _, _, _, _)      => "dataproc"
    case Executor.LOCAL                        => "local"
    case Executor.LIVY(_)                      => "livy"
    case Executor.KUBERNETES(_, _, _, _, _, _) => "kubernetes"
    case Executor.LOCAL_SUBPROCESS(_, _, _)    => "local-subprocess"
  }

  implicit val EtlJob23PropsEncoder: JsonEncoder[EtlJob23Props] = DeriveJsonEncoder.gen
  implicit val EtlJob23PropsDecoder: JsonDecoder[EtlJob23Props] = DeriveJsonDecoder.gen
  implicit val StudentDecoder: JsonDecoder[Student]             = DeriveJsonDecoder.gen
}
