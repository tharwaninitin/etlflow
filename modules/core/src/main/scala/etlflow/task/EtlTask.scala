package etlflow.task

import etlflow.audit.Audit
import etlflow.log.ApplicationLogger
import zio.{RIO, ZIO}
import java.util.concurrent.atomic.AtomicReference

trait EtlTask[R, OP] extends ApplicationLogger {
  val name: String
  val taskType: String = this.getClass.getSimpleName

  def getTaskProperties: Map[String, String] = Map.empty[String, String]

  protected def process: RIO[R, OP]

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  final def execute: RIO[R with Audit, OP] = for {
    tri <- ZIO.succeed(java.util.UUID.randomUUID.toString)
    _   <- Audit.logTaskStart(tri, name, getTaskProperties, taskType)
    op <- process.tapError { ex =>
      Audit.logTaskEnd(tri, name, getTaskProperties, taskType, Some(ex))
    }
    _ <- Audit.logTaskEnd(tri, name, getTaskProperties, taskType, None)
  } yield op

  def map[B](f: OP => B): EtlTask[R, B] = EtlTask.map(this, f)

  def flatMap[R1 <: Any, OP1](f: OP => EtlTask[R1, OP1]): EtlTask[R with R1, OP1] = EtlTask.flatMap[R, OP, R1, OP1](this, f)

  def *>[R1 <: Any, OP1](that: EtlTask[R1, OP1]): EtlTask[R with R1, OP1] = EtlTask.flatMap[R, OP, R1, OP1](this, _ => that)
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
object EtlTask {
  val RT: AtomicReference[Set[String]] = new AtomicReference[Set[String]](Set.empty[String])

  def flatMap[R1, OP1, R2 <: Any, OP2](previousTask: EtlTask[R1, OP1], f: OP1 => EtlTask[R2, OP2]): EtlTask[R1 with R2, OP2] = {
    RT.updateAndGet(_ + previousTask.name)
    new EtlTask[R1 with R2, OP2] {
      override protected def process: RIO[R1 with R2, OP2] = previousTask.process.flatMap(op => f(op).process)
      override val name: String                            = "Pipeline"
    }
  }

  def map[R, A, OP](previousTask: EtlTask[R, A], f: A => OP): EtlTask[R, OP] =
    new EtlTask[R, OP] {
      override protected def process: RIO[R, OP] = previousTask.process.map(f)
      override val name: String                  = previousTask.name
    }
}
