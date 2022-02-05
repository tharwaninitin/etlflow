package etlflow

import zio._
import zio.json._

package object json {

  type JsonEnv = Has[JsonApi.Service]

  case class JsonDecodeException(str: String) extends RuntimeException(str)

  object JsonApi {
    trait Service {
      def convertToObject[T](str: String)(implicit Decoder: JsonDecoder[T]): IO[JsonDecodeException, T]
      def convertToString[A](obj: A)(implicit encoder: JsonEncoder[A]): UIO[String]
    }

    def convertToObject[T](str: String)(implicit Decoder: JsonDecoder[T]): ZIO[JsonEnv, JsonDecodeException, T] =
      ZIO.accessM(_.get.convertToObject[T](str))
    def convertToString[T](obj: T)(implicit encoder: JsonEncoder[T]): URIO[JsonEnv, String] =
      ZIO.accessM(_.get.convertToString[T](obj))
  }
}
