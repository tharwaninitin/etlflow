package etlflow.json

import io.circe.Encoder
import etlflow.schema._

trait JsonImplicits {

  given encodeLoggingLevel: Encoder[LoggingLevel] = Encoder[String].contramap {
    case LoggingLevel.INFO => "info"
    case LoggingLevel.JOB => "job"
    case LoggingLevel.DEBUG => "debug"
  }

  given encodeExecutor: Encoder[Executor] = Encoder[String].contramap {
    case Executor.DATAPROC(_, _, _, _, _) => "dataproc"
    case Executor.LOCAL => "local"
    case Executor.LIVY(_) => "livy"
    case Executor.KUBERNETES(_, _, _, _, _, _) => "kubernetes"
    case Executor.LOCAL_SUBPROCESS(_, _, _) => "local-subprocess"
  }
}