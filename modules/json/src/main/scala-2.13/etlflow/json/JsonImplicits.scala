package etlflow.json

import etlflow.schema.{Executor, LoggingLevel}
import io.circe.Encoder

trait JsonImplicits {

  implicit val encodeLoggingLevel: Encoder[LoggingLevel] = Encoder[String].contramap {
    case LoggingLevel.INFO => "info"
    case LoggingLevel.JOB => "job"
    case LoggingLevel.DEBUG => "debug"
  }

  implicit val encodeExecutor: Encoder[Executor] = Encoder[String].contramap {
    case Executor.DATAPROC(_, _, _, _, _) => "dataproc"
    case Executor.LOCAL => "local"
    case Executor.LIVY(_) => "livy"
    case Executor.KUBERNETES(_, _, _, _, _, _) => "kubernetes"
    case Executor.LOCAL_SUBPROCESS(_, _, _) => "local-subprocess"
  }


//  implicit val encoder_loggingLevel: JsonEncoder[LoggingLevel] = JsonEncoder[String].contramap {
//    case LoggingLevel.INFO => "info"
//    case LoggingLevel.JOB => "job"
//    case LoggingLevel.DEBUG => "debug"
//  }
//
//  implicit val encoder_executor: JsonEncoder[Executor] = JsonEncoder[String].contramap {
//    case Executor.DATAPROC(_, _, _, _, _) => "dataproc"
//    case Executor.LOCAL => "local"
//    case Executor.LIVY(_) => "livy"
//    case Executor.KUBERNETES(_, _, _, _, _, _) => "kubernetes"
//    case Executor.LOCAL_SUBPROCESS(_, _, _) => "local-subprocess"
//  }
}
