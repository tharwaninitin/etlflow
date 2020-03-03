package etljobs.utils

import org.apache.log4j.Logger

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => ru}
import scala.util.control.Exception._

trait UtilityFunctions {
  lazy val uf_logger: Logger = Logger.getLogger(getClass.getName)

  def parser(args: Array[String]): Map[String, String] = {
    args.map {
      case arg => {
        val keyValue = arg.split("==");
        keyValue(0) -> keyValue(1)
      }
    }.toMap
  }
  def printEtlJobs[T: TypeTag]: Unit = {
    val tpe = ru.typeOf[T]
    val clazz = tpe.typeSymbol.asClass
    val allJobNames = clazz.knownDirectSubclasses
    allJobNames.foreach(uf_logger.info(_))
  }
  def getEtlJobName[T: TypeTag](job_name: String): T = {
    val fullClassName = "etljobs.schema.EtlJobList$" + job_name + "$"

    try {
      val classVal = Class.forName(fullClassName)
      val constructor = classVal.getConstructor()
      constructor.newInstance().asInstanceOf[T]
    }
    catch {
      case e: ClassNotFoundException =>
        uf_logger.error(s"Tried creating object with path $fullClassName, but failed with error")
        throw e
    }
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
