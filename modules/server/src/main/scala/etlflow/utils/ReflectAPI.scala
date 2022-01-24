package etlflow.utils

import etlflow.EJPMType
import etlflow.server.model.EtlJob
import etlflow.model.EtlFlowException.EtlJobNotFoundException
import zio.{Tag, Task, UIO, ZIO}

private[etlflow] object ReflectAPI extends ApplicationLogger {

  private[utils] def getTypeFullName[T: Tag]: String =
    implicitly[Tag[T]].tag.longName

  def getFields[T: Tag]: Array[(String, String)] =
    implicitly[Tag[T]].closestClass.getDeclaredFields.map(f => (f.getName, f.getType.getName))

  def getSubClasses[T: Tag]: Task[Set[String]] = Task {
    val tag: Tag[T]               = implicitly[Tag[T]]
    val clazz: Class[_]           = tag.closestClass
    val subClazz: Array[Class[_]] = clazz.getClasses
    subClazz
      .map(x => x.getSimpleName.replace("$", ""))
      .toSet
  }

  def getJob[T: Tag](job_name: String): Task[T] = Task {
    val fullClassName = getTypeFullName[T] + "$" + job_name + "$"
    try {
      val classVal    = Class.forName(fullClassName)
      val constructor = classVal.getConstructor()
      constructor.newInstance().asInstanceOf[T]
    } catch {
      case _: ClassNotFoundException =>
        logger.error(s"Tried creating object with path $fullClassName, but failed with error")
        throw EtlJobNotFoundException(s"$job_name not present")
    }
  }

  def getJobs[T <: EJPMType: Tag]: Task[List[EtlJob]] = {
    val jobs = for {
      jobs    <- getSubClasses[T]
      etljobs <- ZIO.foreach(jobs)(job => getJob[T](job).map(ejpm => EtlJob(job, ejpm.getProps)))
    } yield etljobs.toList
    jobs.tapError { e =>
      UIO(logger.error(e.getMessage))
    }
  }
}
