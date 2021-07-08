package etlflow

import io.circe.{Decoder, Encoder}
import zio.{Has, Task, ZIO}

package object json {

  type JsonEnv = Has[JsonApi.Service]

  object JsonApi {
    trait Service {
      def convertToObject[T](str: String)(implicit Decoder: Decoder[T]): Task[T]
      def convertToString[A](obj: A, Keys:List[String])(implicit encoder: Encoder[A]): Task[String]
      def convertToMap[A](entity: A, Keys:List[String])(implicit encoder: Encoder[A]): Task[Map[String,Any]]
    }

    def convertToObject[T](str: String)(implicit Decoder: Decoder[T]): ZIO[JsonEnv, Throwable, T] =
      ZIO.accessM(_.get.convertToObject[T](str))
    def convertToString[T](obj: T, Keys:List[String])(implicit encoder: Encoder[T]):
      ZIO[JsonEnv, Throwable, String] = ZIO.accessM(_.get.convertToString[T](obj, Keys))
    def convertToMap[T](entity: T, Keys:List[String])(implicit encoder: Encoder[T]):
      ZIO[JsonEnv, Throwable, Map[String,Any]] = ZIO.accessM(_.get.convertToMap[T](entity, Keys))
  }
}
