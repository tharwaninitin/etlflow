package etlflow.json

import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json, parser}
import zio.{Task, UIO, ULayer, ZLayer}

object Implementation {

  private def removeField(json:Json)(keys: List[String]): Json = json.withObject(obj => keys.foldLeft(obj)((acc, s) => acc.remove(s)).asJson)

  lazy val live: ULayer[JsonEnv] = ZLayer.succeed(
    new JsonApi.Service {

      override def convertToObject[T](str: String)(implicit Decoder: Decoder[T]): Task[T] = Task.fromEither {
        parser.decode[T](str)
      }

      override def convertToMap[T](obj: T, keys: List[String])(implicit encoder: Encoder[T]): Task[Map[String, String]] = Task {
        removeField(obj.asJson)(keys).asObject.get.toMap.map{x =>
          (x._1,x._2.toString.replaceAll("\"", ""))
        }
      }

      override def convertToString[T](obj: T, keys: List[String] = List.empty)(implicit encoder: Encoder[T]): UIO[String] = UIO {
        if (keys.isEmpty)
          obj.asJson.noSpaces
        else
          removeField(obj.asJson)(keys).noSpaces
      }
    }
  )
}

