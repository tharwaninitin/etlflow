package etlflow.utils

import EtlflowError.EtlJobNotFoundException
import scala.reflect.runtime.universe.{TypeTag, _}
import scala.reflect.runtime.{universe => ru}

private[etlflow] object ReflectAPI extends ApplicationLogger {

  def printEtlJobs[T: TypeTag](): Unit = {
    val tpe = ru.typeOf[T]
    val clazz = tpe.typeSymbol.asClass
    val allJobNames = clazz.knownDirectSubclasses
    allJobNames.foreach(x => logger.info(x.name.toString))
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

  def getEtlJobPropsMapping[T: TypeTag](job_name: String, etl_job_list_package: String): T = {
    val fullClassName = etl_job_list_package + job_name + "$"
    try {
      val classVal = Class.forName(fullClassName)
      val constructor = classVal.getConstructor()
      constructor.newInstance().asInstanceOf[T]
    }
    catch {
      case e: ClassNotFoundException =>
        logger.error(s"Tried creating object with path $fullClassName, but failed with error")
        throw EtlJobNotFoundException(s"$job_name not present")
    }
  }

  def getFields[T: TypeTag]: Seq[(String, String)] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => (m.name.toString, m.returnType.toString)
  }.toSeq

  def stringFormatter(value: String):String = value.take(50).replaceAll("[^a-zA-Z0-9]", " ").replaceAll("\\s+", "_").toLowerCase

}
