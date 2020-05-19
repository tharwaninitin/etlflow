package etlflow.utils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import etlflow.{EtlJobName, EtlJobNotFoundException, EtlJobProps}
import org.apache.log4j.Logger
import org.json4s.JsonAST.JNothing
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.writePretty
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, FieldSerializer, JValue, _}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, _}
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

object UtilityFunctions {
  lazy val uf_logger: Logger = Logger.getLogger(getClass.getName)

  def parser(args: Array[String]): Map[String, String] = {
    args.map {
      case arg => {
        val keyValue = arg.split("==");
        keyValue(0) -> keyValue(1)
      }
    }.toMap
  }

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
    // https://stackoverflow.com/questions/22179915/json4s-support-for-case-class-with-trait-mixin
    val customSerializer2 = new FieldSerializer[EtlJobProps]
    implicit val formats = DefaultFormats + customSerializer1 + customSerializer2
    writePretty(Extraction.decompose(entity).removeField { x => keys.contains(x._1)})
  }

  def convertToJsonByRemovingKeysAsMap(entity: AnyRef, keys: List[String]): Map[String,Any] = {
    // https://stackoverflow.com/questions/36333316/json4s-ignore-field-of-particular-type-during-serialization
    val customSerializer1 = new CustomSerializer[EtlJobName[EtlJobProps]](_ =>
      (PartialFunction.empty, { case _: EtlJobName[_] => JNothing })
    )
    // https://stackoverflow.com/questions/22179915/json4s-support-for-case-class-with-trait-mixin
    val customSerializer2 = new FieldSerializer[EtlJobProps]
    implicit val formats: Formats = DefaultFormats + customSerializer1 + customSerializer2
    val json: JValue = Extraction.decompose(entity).removeField { x => keys.contains(x._1)}
    parse(writePretty(json)).extract[Map[String, Any]]
  }

  def getCurrentTimestamp: Long = System.currentTimeMillis()

  def getCurrentTimestampAsString(pattern: String): String = DateTimeFormatter.ofPattern(pattern).format(LocalDateTime.now)

  def roundAt(p: Int)(n: Double): Double = { val s = math pow (10, p); (math round n * s) / s }

  def getTimeDifferenceAsString(start_ts: Long, end_ts: Long): String = {
    Try((end_ts - start_ts) / 1000.0).map{value =>
      if (value > 60) roundAt(2)(value/60.0) + " mins"
      else roundAt(2)(value) + " secs"
    } match {
      case Success(value) => value
      case Failure(e) =>
        uf_logger.error(s"Error in converting ts(Long) to String, ${e.getMessage}")
        (end_ts - start_ts).toString
    }
  }

  def printEtlJobs[T: TypeTag]: Unit = {
    val tpe = ru.typeOf[T]
    val clazz = tpe.typeSymbol.asClass
    val allJobNames = clazz.knownDirectSubclasses
    allJobNames.foreach(x => uf_logger.info(x.name))
  }

  def getEtlJobs[T: TypeTag]: Set[String] = {
    val tpe = ru.typeOf[T]
    val clazz = tpe.typeSymbol.asClass
    val allJobNames = clazz.knownDirectSubclasses
    allJobNames.map(x => x.name.toString)
  }

  def getEtlJobProps[T: TypeTag](excludeColumnList: Set[String] = Set("job_run_id","job_description","job_properties","job_name")): Map[String,String] =
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => (m.name.toString, m.returnType.toString)
      case m: MethodSymbol if (m.isVar || m.isVal) && m.isGetter => (m.name.toString, m.returnType.toString)
    }.toList.filterNot(x => excludeColumnList.contains(x._1)).toMap

  def getEtlJobName[T: TypeTag](job_name: String, etl_job_list_package: String = "etlflow.schema.EtlJobList$"): T = {
    val fullClassName = etl_job_list_package + job_name + "$"
    try {
      val classVal = Class.forName(fullClassName)
      val constructor = classVal.getConstructor()
      constructor.newInstance().asInstanceOf[T]
    }
    catch {
      case e: ClassNotFoundException =>
        uf_logger.error(s"Tried creating object with path $fullClassName, but failed with error")
        throw EtlJobNotFoundException(s"$job_name not present")
    }
  }

  def getEtlJobNameGeneric[T: TypeTag](job_name: String): T = {
    val tpe = ru.typeOf[T]
    val clazz = tpe.typeSymbol.asClass
    val allJobNames = clazz.knownDirectSubclasses
    val fullName = allJobNames.filter(x => x.name.toString.contains(job_name)).head.fullName
    val name = allJobNames.filter(x => x.name.toString.contains(job_name)).head.name.toString
    val finalName = fullName.replace("." + name,"$" + name + "$")
    val classVal = Class.forName(finalName)
    val constructor = classVal.getConstructor()
    constructor.newInstance().asInstanceOf[T]
  }

  def getGlobalPropertiesUsingReflection[T <: GlobalProperties](path: String = "loaddata.properties")(implicit tag: ClassTag[T]): Option[T] = {
    Try {
      tag.runtimeClass.getConstructor(classOf[String]).newInstance(path).asInstanceOf[T]
    } match {
      case Success(value) => Some(value)
      case Failure(exception) =>
        uf_logger.info(exception.printStackTrace())
        None
    }
  }

  def getGlobalProperties[T <: GlobalProperties](path: String = "loaddata.properties")(fct: String => T): Option[T] = {
    Try {
      fct(path)
    } match {
      case Success(value) => Some(value)
      case Failure(exception) =>
        uf_logger.info(exception.getMessage)
        None
    }
  }
}
