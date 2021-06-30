package etlflow.json

import etlflow.schema.{ Executor, LoggingLevel}
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

//  implicit val encodeProps: Encoder[EtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]] = Encoder[String].contramap {
//    case _: EtlJobPropsMapping[_,_] => ""
//  }
}
