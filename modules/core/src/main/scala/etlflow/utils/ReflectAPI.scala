package etlflow.utils

import etlflow.EJPMType
import etlflow.db.EtlJob
import etlflow.utils.EtlflowError.EtlJobNotFoundException
import zio.{Task, UIO, ZIO}
import scala.reflect.runtime.universe.{TypeTag, _}
import scala.reflect.runtime.{universe => ru}

private[etlflow] object ReflectAPI extends ApplicationLogger {

  def printEtlJobs[T: TypeTag](): Task[Unit] = Task{
    val tpe = ru.typeOf[T]
    val clazz = tpe.typeSymbol.asClass
    val allJobNames = clazz.knownDirectSubclasses
    allJobNames.foreach(x => logger.info(x.name.toString))
  }

  def getJobNamePackage[T: TypeTag]: Task[String] = Task{
    val tpe = ru.typeOf[T]
    tpe.typeSymbol.asClass.fullName
  }

  private[utils] def getEtlJobs[T: TypeTag]: Task[Set[String]] = Task{
    val tpe = ru.typeOf[T]
    val clazz = tpe.typeSymbol.asClass
    val allJobNames = clazz.knownDirectSubclasses
    allJobNames.map(x => x.name.toString)
  }

  def getEtlJobPropsMapping[T: TypeTag](job_name: String, etl_job_list_package: String): Task[T] = Task{
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

  def getFields[T: TypeTag]: Task[Seq[(String, String)]] = Task {
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => (m.name.toString, m.returnType.toString)
    }.toSeq
  }

  // Memoize this function by creating cache and if key exists return from cache or else call below function
  def getJobPropsMapping[EJN <: EJPMType : TypeTag](job_name: String, ejpm_package: String): Task[Map[String, String]] = {
    getEtlJobPropsMapping[EJN](job_name, ejpm_package).map { props_mapping =>
      props_mapping.getProps.map(x => {
        (x._1, x._2.toString)
      })
    }
  }

  def getEtlJobs[EJN <: EJPMType : TypeTag](ejpm_package: String): Task[List[EtlJob]] = {
    val jobs = for {
      jobs     <- getEtlJobs[EJN]
      etljobs  <- ZIO.foreach(jobs)(job => getJobPropsMapping[EJN](job,ejpm_package).map(kv => EtlJob(job,kv)))
    } yield etljobs.toList
    jobs.tapError{ e =>
      UIO(logger.error(e.getMessage))
    }
  }
}
