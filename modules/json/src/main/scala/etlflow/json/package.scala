package etlflow

import io.circe.{Decoder, Encoder}
import zio.{Has, Task, UIO, URIO, ZIO}

package object json {

  type JsonEnv = Has[JsonApi.Service]

  object JsonApi {
    trait Service {
      def convertToObject[T](str: String)(implicit Decoder: Decoder[T]): Task[T]
      def convertToString[A](obj: A, keys: List[String])(implicit encoder: Encoder[A]): UIO[String]
      def convertToMap[A](obj: A, keys: List[String])(implicit encoder: Encoder[A]): Task[Map[String, String]]
    }

    def convertToObject[T](str: String)(implicit Decoder: Decoder[T]): ZIO[JsonEnv, Throwable, T] =
      ZIO.accessM(_.get.convertToObject[T](str))
    def convertToString[T](obj: T, keys: List[String] = List.empty)(implicit
        encoder: Encoder[T]
    ): URIO[JsonEnv, String] = ZIO.accessM(_.get.convertToString[T](obj, keys))
    def convertToMap[T](obj: T, keys: List[String] = List.empty)(implicit
        encoder: Encoder[T]
    ): ZIO[JsonEnv, Throwable, Map[String, String]] = ZIO.accessM(_.get.convertToMap[T](obj, keys))
  }
}
