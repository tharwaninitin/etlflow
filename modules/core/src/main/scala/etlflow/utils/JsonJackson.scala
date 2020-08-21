package etlflow.utils

import etlflow.utils.Environment.LOCAL
import etlflow.utils.Executor.DATAPROC
import etlflow.{EtlJobName, EtlJobProps}
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, FieldSerializer, Formats, JValue}
import org.json4s.JsonAST.{JNothing, JString}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.writePretty
import org.slf4j.{Logger, LoggerFactory}

object JsonJackson {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def convertToJson(entity: AnyRef): String = {
    implicit val formats = DefaultFormats
    writePretty(entity)
  }

  // https://stackoverflow.com/questions/29296335/json4s-jackson-how-to-ignore-field-using-annotations
  def convertToJsonByRemovingKeys(entity: AnyRef, keys: List[String]): String = {

    // https://stackoverflow.com/questions/36333316/json4s-ignore-field-of-particular-type-during-serialization
    val customSerializer1 = new CustomSerializer[EtlJobName[EtlJobProps]](_ =>
      (PartialFunction.empty, { case _: EtlJobName[_] => JNothing })
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

    val customSerializer4 = new CustomSerializer[Executor](formats =>
      ({
        case JString(s) => s match {
          case "dataproc" => Executor.DATAPROC("","","","")
          case "local" => Executor.LOCAL
          case "livy" => Executor.LIVY("")
          case "kubernetes"   => Executor.KUBERNETES("","",Map.empty)
        }
      }, {
        case executor: Executor => executor match{
          case Executor.DATAPROC(project, region, endpoint, cluster_name) => {
            val dataprocList = Map("project name" -> project,"Region" -> region,"endpoint" -> endpoint,"cluster_name" -> cluster_name)
            JString(dataprocList.toString())
          }
          case Executor.LOCAL => JString("local")
          case Executor.KUBERNETES(imageName, nameSpace, envVar, containerName, entryPoint, restartPolicy)=> {
            val k8List = Map("imageName" -> imageName,"nameSpace" -> nameSpace,"envVar" -> envVar,"containerName" -> containerName,"entryPoint" -> entryPoint,"restartPolicy" -> restartPolicy)
            JString(k8List.toString())
          }
        }
      })
    )




    // https://stackoverflow.com/questions/22179915/json4s-support-for-case-class-with-trait-mixin
    val customSerializer2 = new FieldSerializer[EtlJobProps]
    implicit val formats = DefaultFormats + customSerializer1 + customSerializer2 + customSerializer3 + customSerializer4
    writePretty(Extraction.decompose(entity).removeField { x => keys.contains(x._1)})
  }

  def convertToJsonByRemovingKeysAsMap(entity: AnyRef, keys: List[String]): Map[String,Any] = {

    // https://stackoverflow.com/questions/36333316/json4s-ignore-field-of-particular-type-during-serialization
    val customSerializer1 = new CustomSerializer[EtlJobName[EtlJobProps]](_ =>
      (PartialFunction.empty, { case _: EtlJobName[_] => JNothing })
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

    val customSerializer4 = new CustomSerializer[Executor](formats =>
      ({
        case JString(s) => s match {
          case "dataproc" => Executor.DATAPROC("","","","")
          case "local" => Executor.LOCAL
          case "livy" => Executor.LIVY("")
          case "kubernetes"   => Executor.KUBERNETES("","",Map.empty)
        }
      }, {
        case executor: Executor => executor match{
          case Executor.DATAPROC(project, region, endpoint, cluster_name) => {
            val dataprocList = Map("project name" -> project,"Region" -> region,"endpoint" -> endpoint,"cluster_name" -> cluster_name)
            JString(dataprocList.toString())
          }
          case Executor.LOCAL => JString("local")
          case Executor.KUBERNETES(imageName, nameSpace, envVar, containerName, entryPoint, restartPolicy)=> {
            val k8List = Map("imageName" -> imageName,"nameSpace" -> nameSpace,"envVar" -> envVar,"containerName" -> containerName,"entryPoint" -> entryPoint,"restartPolicy" -> restartPolicy)
            JString(k8List.toString())
          }
        }
      })
    )


    // https://stackoverflow.com/questions/22179915/json4s-support-for-case-class-with-trait-mixin
    val customSerializer2 = new FieldSerializer[EtlJobProps]
    implicit val formats: Formats = DefaultFormats + customSerializer1 + customSerializer2 + customSerializer3 + customSerializer4
    val json: JValue = Extraction.decompose(entity).removeField { x => keys.contains(x._1)}
    val x = writePretty(json)
    parse(writePretty(json)).extract[Map[String, Any]]
  }

  // https://stackoverflow.com/questions/14661811/json4s-unknown-error
  def convertToObject[T](str: String, fmt: Formats = DefaultFormats)(implicit mf: Manifest[T]): T = {
    val json = parse(str)
    logger.info("Parsed AST => " + writePretty(json)(fmt))
    Extraction.extract(json)(fmt,mf)
  }

}
