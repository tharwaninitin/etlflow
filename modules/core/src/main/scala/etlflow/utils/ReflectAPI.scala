package etlflow.utils

import EtlflowError.EtlJobNotFoundException

import scala.reflect.runtime.universe.{TypeTag, _}
import scala.reflect.runtime.{universe => ru}
import etlflow.EJPMType
import etlflow.cache.CacheApi
import etlflow.db.EtlJob
import zio.{Task, UIO, ZEnv, ZIO}
import zio.Runtime.default.unsafeRun

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

  def getEtlJobs[T: TypeTag]: Task[Set[String]] = Task{
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

  val propCache = CacheApi.createCache[Map[String, String]].provideCustomLayer(etlflow.cache.Implementation.live)

  private [etlflow] def isCached(job_name: String): ZIO[ZEnv, Throwable, Option[Map[String, String]]] =
    CacheApi.get[Map[String, String]](unsafeRun(propCache), job_name).provideCustomLayer(etlflow.cache.Implementation.live)

  // Memoize this function by creating cache and if key exists return from cache or else call below function
  def getJobPropsMapping[EJN <: EJPMType : TypeTag](job_name: String, ejpm_package: String): Task[Map[String, String]] = {
//    unsafeRun(isCached(job_name)) match {
//      case Some(value) => Task(value)
//      case None =>
//        logger.info(s"$job_name Not Present in Cache")
//        getEtlJobPropsMapping[EJN](job_name, ejpm_package).map { props_mapping =>
//          props_mapping.getProps.map(x => {
//            CacheApi.put[Map[String, String]](unsafeRun(propCache), job_name, (x._1, x._2.toString))
//            (x._1, x._2.toString)
//          })
//        }
//    }
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
