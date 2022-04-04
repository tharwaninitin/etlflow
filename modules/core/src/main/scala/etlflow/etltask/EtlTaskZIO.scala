package etlflow.etltask

import etlflow.log.{LogApi, LogEnv}
import etlflow.utils.DateTimeApi
import zio.{RIO, ZIO}

trait EtlTaskZIO[R, OP] extends EtlTask {

  protected def processZio: RIO[R, OP]

  final def executeZio: RIO[R with LogEnv, OP] = for {
    sri <- ZIO.succeed(java.util.UUID.randomUUID.toString)
    _   <- LogApi.logStepStart(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp)
    op <- processZio.tapError { ex =>
      LogApi.logStepEnd(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp, Some(ex))
    }
    _ <- LogApi.logStepEnd(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp, None)
  } yield op
}
