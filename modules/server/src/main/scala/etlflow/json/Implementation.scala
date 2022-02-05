package etlflow.json

import zio._
import zio.json._

object Implementation {
  lazy val live: ULayer[JsonEnv] = ZLayer.succeed(
    new JsonApi.Service {
      override def convertToObject[T](str: String)(implicit Decoder: JsonDecoder[T]): IO[JsonDecodeException, T] =
        IO.fromEither(str.fromJson[T]).mapError(str => JsonDecodeException(str))
      override def convertToString[T](obj: T)(implicit encoder: JsonEncoder[T]): UIO[String] = UIO(obj.toJson)
    }
  )
}
