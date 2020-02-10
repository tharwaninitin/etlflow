package etljobs.utils

import org.apache.log4j.Logger

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.universe.TypeTag

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
    import scala.reflect.runtime.{universe => ru}
    val tpe = ru.typeOf[T]
    val clazz = tpe.typeSymbol.asClass
    val allJobNames = clazz.knownDirectSubclasses
    allJobNames.foreach(uf_logger.info(_))
  }
  //  val job_name = getEtlJobName(job_properties("job_name"))
  //  def getEtlJobName(job_name: String): Option[MyEtlJobName] = {
  //    val job = MyEtlJobName.fromString(job_name)
  //    job
  //  }
  //  val my_global_properties = getGlobalProperties[MyGlobalProperties]("loaddata.properties")
  //  val my_global_properties = getGlobalProperties("loaddata.properties")(new MyGlobalProperties(_))
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
