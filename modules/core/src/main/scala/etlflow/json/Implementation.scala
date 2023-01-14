package etlflow.json

import etlflow.model.EtlFlowException.JsonDecodeException
import zio._
import zio.json._

object Implementation {
  lazy val live: ULayer[JsonApi] = ZLayer.succeed(
    new JsonApi {
      override def convertToObject[T](str: String)(implicit Decoder: JsonDecoder[T]): IO[JsonDecodeException, T] =
        ZIO.fromEither(str.fromJson[T]).mapError(str => JsonDecodeException(str))
      override def convertToString[T](obj: T)(implicit encoder: JsonEncoder[T]): UIO[String] = ZIO.succeed(obj.toJson)
    }
  )
}
