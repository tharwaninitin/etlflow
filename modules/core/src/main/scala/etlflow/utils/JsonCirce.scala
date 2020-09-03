package etlflow.utils

import etlflow.{EtlJobName, EtlJobProps}
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json, parser}
object JsonCirce  {

  implicit val customSerializer2: Encoder[EtlJobName[EtlJobProps]] = Encoder[String].contramap {
    case _: EtlJobName[_] => ""
  }

  implicit val customSerializer3: Encoder[LoggingLevel] = Encoder[String].contramap {
    case LoggingLevel.INFO => "info"
    case LoggingLevel.DEBUG => "debug"
    case LoggingLevel.JOB => "job"
  }

  implicit val customSerializer4: Encoder[Executor] = Encoder[String].contramap {
    case Executor.DATAPROC(_, _, _, _) => "dataproc"
    case Executor.LOCAL            => "local"
    case Executor.LIVY(_) => "livy"
    case Executor.KUBERNETES(_, _, _, _, _, _)=> "kubernetes"
  }


  def removeField(json:Json)(keys:List[String]):Json=
    json.withObject(obj=>keys.foldLeft(obj)((acc, s)=>acc.remove(s)).asJson)

  def convertToJson[A](obj: A)(implicit encoder: Encoder[A]): String =
    obj.asJson.noSpaces

  def convertToJsonByRemovingKeysAsMap[A <: EtlJobProps](entity: A, Keys:List[String])(implicit encoder: Encoder[A]): Map[String,Any] = {
    convertToJsonByRemovingKeysAsJson(entity,Keys).asObject.get.toMap.mapValues(x => {
      if ("true".equalsIgnoreCase(x.toString()) || "false".equalsIgnoreCase(x.toString())) {
        x.asBoolean.get
      }else{
        x.asString.get
      }
    })
  }

  private def convertToJsonByRemovingKeysAsJson[A <: EtlJobProps](entity: A, Keys:List[String])(implicit encoder: Encoder[A]): Json = {

    val caseClassJson  = parse(convertToJson(entity)).toOption.get
    val etlPropsJson = Json.obj(
      "job_enable_db_logging"   -> Json.fromBoolean(entity.job_enable_db_logging),
      "job_schedule" -> Json.fromString(entity.job_schedule),
      "job_send_slack_notification" -> Json.fromBoolean(entity.job_send_slack_notification),
      "job_notification_level" -> Json.fromString(entity.job_notification_level match {
        case LoggingLevel.INFO => "info"
        case LoggingLevel.DEBUG => "debug"
        case LoggingLevel.JOB => "job"
      }),
      "job_deploy_mode" -> Json.fromString(entity.job_deploy_mode match {
        case Executor.DATAPROC(_, _, _, _) => "dataproc"
        case Executor.LOCAL            => "local"
        case Executor.LIVY(_) => "livy"
        case Executor.KUBERNETES(_, _, _, _, _, _)=> "kubernetes"
      })
    )
    val combineJson = for {
      parsedJsonString <- parse(convertToJson(caseClassJson.deepMerge(etlPropsJson)))
      cleanedUp = removeField(parsedJsonString)(Keys)
    } yield cleanedUp

    combineJson.toOption.get
  }

  def convertToJsonByRemovingKeys[A <: EtlJobProps](entity: A, Keys:List[String])(implicit encoder: Encoder[A]): String = {
    convertToJsonByRemovingKeysAsJson(entity,Keys).toString()
  }

  def convertToObject[T](str: String)(implicit Decoder: Decoder[T]): T = {
    val decodeResult1 = parser.decode[T](str)
    decodeResult1.toOption.get
  }
}
