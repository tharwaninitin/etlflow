package etlflow.json

import etlflow.etljobs.EtlJob
import etlflow.utils.{Executor, LoggingLevel}
import etlflow.{EtlJobProps, EtlJobPropsMapping}
import io.circe
import io.circe.parser.parse
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json, parser}
import org.json4s.JsonAST.{JNothing, JString}
import org.json4s.jackson.Serialization.writePretty
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, FieldSerializer, Formats, JValue}
import zio.{Task, ULayer, ZLayer}
import org.json4s.jackson.JsonMethods.{parse => JacksonParse}

object Implementation {

  val live: ULayer[JsonEnv] = ZLayer.succeed(
    new JsonApi.Service {
      override def convertToObject[T](str: String)(implicit Decoder: Decoder[T]): Task[T] = Task{
        val decodeResult1 = parser.decode[T](str)
        decodeResult1.toOption.get
      }
      override def convertToJsonByRemovingKeysAsMap[T](entity: T, Keys: List[String])(implicit encoder: Encoder[T]): Task[Map[String, Any]] = Task {
        val parsedJsonString = parse(entity.asJson.noSpaces).toOption.get
        removeField(parsedJsonString)(Keys).asObject.get.toMap.mapValues(x => {
          if ("true".equalsIgnoreCase(x.toString()) || "false".equalsIgnoreCase(x.toString())) {
            x.asBoolean.get
          }else{
            x.asString.get
          }
        })
      }
      override def convertToJsonByRemovingKeys[T](obj: T, Keys: List[String])(implicit encoder: Encoder[T]): Task[Json] = Task {
        val parsedJsonString = parse(obj.asJson.noSpaces).toOption.get
        removeField(parsedJsonString)(Keys)
      }

      override def convertToJson[A](obj: A)(implicit encoder: Encoder[A]): Task[String] = Task{
        obj.asJson.noSpaces
      }

      override def convertToJsonJacksonByRemovingKeys(entity: AnyRef, keys: List[String]): Task[String] = Task{
        val customSerializer1 = new CustomSerializer[EtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]](_ =>
          (PartialFunction.empty, { case _: EtlJobPropsMapping[_,_] => JNothing })
        )

        val customSerializer3 = new CustomSerializer[LoggingLevel](formats =>
          ( {
            case JString(s) => s match {
              case "info" => LoggingLevel.INFO
              case "debug" => LoggingLevel.DEBUG
              case "job" => LoggingLevel.JOB
            }
          }, {
            case loggingLevel: LoggingLevel => loggingLevel match {
              case LoggingLevel.INFO => JString("info")
              case LoggingLevel.DEBUG => JString("debug")
              case LoggingLevel.JOB => JString("job")
            }
          })
        )

        val customSerializer4 = new CustomSerializer[Executor](_ =>
          (PartialFunction.empty, {
            case executor: Executor => executor match {
              case Executor.DATAPROC(_, _, _, _, _) => JString("dataproc")
              case Executor.LOCAL => JString("local")
              case Executor.LIVY(_) => JString("livy")
              case Executor.KUBERNETES(_, _, _, _, _, _)=> JString("kubernetes")
              case Executor.LOCAL_SUBPROCESS(_,_,_)=> JString("local-subprocess")
            }
          })
        )

        // https://stackoverflow.com/questions/22179915/json4s-support-for-case-class-with-trait-mixin
        val customSerializer2 = new FieldSerializer[EtlJobProps]
        implicit val formats = DefaultFormats + customSerializer1 + customSerializer2 + customSerializer3 + customSerializer4
        writePretty(Extraction.decompose(entity).removeField { x => keys.contains(x._1)})
      }

      override def convertToJsonJacksonByRemovingKeysAsMap(entity: AnyRef, keys: List[String]): Task[Map[String, Any]] = Task{

        // https://stackoverflow.com/questions/36333316/json4s-ignore-field-of-particular-type-during-serialization
        val customSerializer1 = new CustomSerializer[EtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]](_ =>
          (PartialFunction.empty, { case _: EtlJobPropsMapping[_,_] => JNothing })
        )

        val customSerializer3 = new CustomSerializer[LoggingLevel](formats =>
          ( {
            case JString(s) => s match {
              case "info" => LoggingLevel.INFO
              case "debug" => LoggingLevel.DEBUG
              case "job" => LoggingLevel.JOB
            }
          }, {
            case loggingLevel: LoggingLevel => loggingLevel match {
              case LoggingLevel.INFO => JString("info")
              case LoggingLevel.DEBUG => JString("debug")
              case LoggingLevel.JOB => JString("job")
            }
          })
        )

        val customSerializer4 = new CustomSerializer[Executor](_ =>
          (PartialFunction.empty, {
            case executor: Executor => executor match {
              case Executor.DATAPROC(_, _, _, _, _) => JString("dataproc")
              case Executor.LOCAL => JString("local")
              case Executor.LIVY(_) => JString("livy")
              case Executor.KUBERNETES(_, _, _, _, _, _)=> JString("kubernetes")
              case Executor.LOCAL_SUBPROCESS(_,_,_)=> JString("local-subprocess")

            }
          })
        )

        // https://stackoverflow.com/questions/22179915/json4s-support-for-case-class-with-trait-mixin
        val customSerializer2 = new FieldSerializer[EtlJobProps]
        implicit val formats: Formats = DefaultFormats + customSerializer1 + customSerializer2 + customSerializer3 + customSerializer4
        val json: JValue = Extraction.decompose(entity).removeField { x => keys.contains(x._1)}
        JacksonParse(writePretty(json)).extract[Map[String, Any]]
      }

      override def convertToObjectUsingJackson[T](str: String, fmt: Formats = DefaultFormats)(implicit mf: Manifest[T]): Task[T] = Task{
        val json = JacksonParse(str)
        Extraction.extract(json)(fmt,mf)
      }

      override def convertToObjectEither[T](str: String)(implicit Decoder: Decoder[T]): Task[Either[circe.Error, T]] = Task{
        parser.decode[T](str)
      }
    })
}
