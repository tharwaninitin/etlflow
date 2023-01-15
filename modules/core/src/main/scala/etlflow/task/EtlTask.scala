package etlflow.task

import etlflow.audit.Audit
import etlflow.job.EtlJob
import etlflow.log.ApplicationLogger
import zio.{RIO, ZIO}

trait EtlTask[R, OP] extends ApplicationLogger {
  val name: String
  val taskType: String = this.getClass.getSimpleName

  def getTaskProperties: Map[String, String] = Map.empty[String, String]

  protected def process: RIO[R, OP]

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  final def execute: RIO[R with Audit, OP] = for {
    tri <- ZIO.succeed(java.util.UUID.randomUUID.toString)
    _   <- Audit.logTaskStart(tri, name, getTaskProperties, taskType)
    op  <- process.tapError(ex => Audit.logTaskEnd(tri, name, getTaskProperties, taskType, Some(ex)))
    _   <- Audit.logTaskEnd(tri, name, getTaskProperties, taskType, None)
  } yield op

  /** Experimental method map for EtlTask, don't use in production
    */
  def map[B](f: OP => B): EtlTask[R, B] = EtlTask.map(this, f)

  /** Experimental method flatMap for EtlTask to convert to EtlJob, don't use in production
    */
  def flatMap[R1, OP1](fn: OP => EtlTask[R1, OP1]): EtlJob[R with R1, OP1] = EtlTask.flatMap[R, OP, R1, OP1](this, fn)

  /** Experimental method *> (variant of flatMap that ignores the value produced by this effect) for EtlTask to convert to EtlJob,
    * don't use in production
    */
  def *>[R1, OP1](that: EtlTask[R1, OP1]): EtlJob[R with R1, OP1] = EtlTask.flatMap[R, OP, R1, OP1](this, _ => that)
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.AsInstanceOf"))
object EtlTask {

  /** Experimental method map for EtlTask, don't use in production
    */
  def flatMap[R1, OP1, R2, OP2](currentTask: EtlTask[R1, OP1], fn: OP1 => EtlTask[R2, OP2]): EtlJob[R1 with R2, OP2] =
    new EtlJob[R1 with R2, OP2] {
      override protected def process: RIO[R1 with R2 with Audit, OP2] = currentTask.execute.flatMap(op => fn(op).execute)
    }

  /** Experimental method flatMap for EtlTask to convert to EtlJob, don't use in production
    */
  def map[R, A, OP](currentTask: EtlTask[R, A], fn: A => OP): EtlTask[R, OP] =
    new EtlTask[R, OP] {
      override protected def process: RIO[R, OP] = currentTask.process.map(fn)
      override val name: String                  = currentTask.name
      override val taskType: String              = currentTask.taskType
    }
}
