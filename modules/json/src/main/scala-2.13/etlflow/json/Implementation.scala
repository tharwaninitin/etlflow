package etlflow.json

import io.circe.parser.parse
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json, parser}
import zio.{Task, ULayer, ZLayer}

object Implementation {

  private def removeField(json:Json)(keys:List[String]): Json = json.withObject(obj=>keys.foldLeft(obj)((acc, s)=>acc.remove(s)).asJson)

  val live: ULayer[JsonEnv] = ZLayer.succeed(
    new JsonApi.Service {

      override def convertToObject[T](str: String)(implicit Decoder: Decoder[T]): Task[T] = Task.fromEither {
        parser.decode[T](str)
      }

      override def convertToMap[T](entity: T, keys: List[String])(implicit encoder: Encoder[T]): Task[Map[String, Any]] = Task {
        val parsedJsonString = parse(entity.asJson.noSpaces).toOption.get
        removeField(parsedJsonString)(keys).asObject.get.toMap.view.mapValues(x => {
          if ("true".equalsIgnoreCase(x.toString()) || "false".equalsIgnoreCase(x.toString())) {
            x.asBoolean.get
          } else {
            x.asString.get
          }
        }).toMap
      }

      override def convertToString[T](obj: T, keys: List[String] = List.empty)(implicit encoder: Encoder[T]): Task[String] = Task {
        if (keys.isEmpty)
          obj.asJson.noSpaces
        else {
          val parsedJsonString = parse(obj.asJson.noSpaces).toOption.get
          removeField(parsedJsonString)(keys).toString()
        }
      }

    }
  )
}


//  def convertToObject[T](str: String)(implicit Decoder: JsonDecoder[T]): Task[Either[String,T]] = Task{
//    str.fromJson[T]
//  }
//
//  def convertToString[T](obj: T, keys: List[String])(implicit encoder: JsonEncoder[T]): Task[String] = Task{
//    obj.toJson
//  }
//
//  def convertToMap[T](entity: T, Keys:List[String]): Task[Map[String, Any]] = Task{
//    entity.getClass.getDeclaredFields.foldLeft(Map.empty[String, Any]) { (a, f) =>
//      f.setAccessible(true)
//      a + (f.getName -> f.get(entity))
//    }
//  }
