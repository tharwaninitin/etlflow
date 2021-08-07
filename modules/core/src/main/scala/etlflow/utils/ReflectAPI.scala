package etlflow.utils

import etlflow.EJPMType
import etlflow.db.EtlJob
import etlflow.utils.EtlflowError.EtlJobNotFoundException
import zio.{Task, UIO, ZIO}
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}

private[etlflow] object ReflectAPI extends ApplicationLogger {

  private[utils] def getTypeFullName[T: TypeTag]: String = {
    val tpe = ru.typeOf[T]
    tpe.typeSymbol.asClass.fullName
  }

  def getSubClasses[T: TypeTag]: Task[Set[String]] = Task {
    val tpe = ru.typeOf[T]
    val clazz = tpe.typeSymbol.asClass
    val allJobNames = clazz.knownDirectSubclasses
    allJobNames.map(x => x.name.toString)
  }

  def getJob[T: TypeTag](job_name: String): Task[T] = Task {
    val fullClassName = getTypeFullName[T] + "$" + job_name + "$"
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

  def getJobs[T <: EJPMType : TypeTag]: Task[List[EtlJob]] = {
    val jobs = for {
      jobs     <- getSubClasses[T]
      etljobs  <- ZIO.foreach(jobs)(job => getJob[T](job).map(ejpm => EtlJob(job,ejpm.getProps)))
    } yield etljobs.toList
    jobs.tapError{ e =>
      UIO(logger.error(e.getMessage))
    }
  }

  def getFields[T: TypeTag]: Task[Seq[(String, String)]] = Task {
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => (m.name.toString, m.returnType.toString)
    }.toSeq
  }
}
