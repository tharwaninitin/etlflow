package etlflow.task

import etlflow.log.{LogApi, LogEnv}
import etlflow.utils.DateTimeApi
import zio.{RIO, ZIO}

trait EtlTaskZIO[R, OP] extends EtlTask {

  protected def processZio: RIO[R, OP]

  final def executeZio: RIO[R with LogEnv, OP] = for {
    sri <- ZIO.succeed(java.util.UUID.randomUUID.toString)
    _   <- LogApi.logTaskStart(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp)
    op <- processZio.tapError { ex =>
      LogApi.logTaskEnd(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp, Some(ex))
    }
    _ <- LogApi.logTaskEnd(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp, None)
  } yield op
}
