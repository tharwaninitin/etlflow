package etlflow.utils

import com.github.t3hnar.bcrypt._
import etlflow.common.EtlflowError.EtlJobNotFoundException
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.runtime.universe.{TypeTag, _}
import scala.reflect.runtime.{universe => ru}

object UtilityFunctions{
  lazy val uf_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def parser(args: Array[String]): Map[String, String] = {
    args.map {
      case arg => {
        val keyValue = arg.split("==");
        keyValue(0) -> keyValue(1)
      }
    }.toMap
  }

  def printEtlJobs[T: TypeTag](): Unit = {
    val tpe = ru.typeOf[T]
    val clazz = tpe.typeSymbol.asClass
    val allJobNames = clazz.knownDirectSubclasses
    allJobNames.foreach(x => uf_logger.info(x.name.toString))
  }

  def getJobNamePackage[T: TypeTag]: String = {
    val tpe = ru.typeOf[T]
    tpe.typeSymbol.asClass.fullName
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

  def getEtlJobName[T: TypeTag](job_name: String, etl_job_list_package: String): T = {
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

  def getFields[T: TypeTag]: Seq[(String, String)] = typeOf[T].members.collect {
     case m: MethodSymbol if m.isCaseAccessor => (m.name.toString, m.returnType.toString)
  }.toSeq

  def stringFormatter(value: String):String = value.take(50).replaceAll("[^a-zA-Z0-9]", " ").replaceAll("\\s+", "_").toLowerCase

  def encryptKey(key:String) =  {
    val salt = BCrypt.gensalt()
    key.bcryptBounded(salt)
  }
}
