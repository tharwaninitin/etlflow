package etlflow.job

import etlflow.audit.Audit
import etlflow.task.EtlTask
import zio.RIO

/** Experimental EtlJob API, don't use in production
  */
trait EtlJob[R, OP] {

  protected def process: RIO[R with Audit, OP]

  final def execute(name: String, props: Map[String, String]): RIO[R with Audit, OP] = for {
    _  <- Audit.logJobStart(name, props)
    op <- process.tapError(ex => Audit.logJobEnd(name, props, Some(ex)))
    _  <- Audit.logJobEnd(name, props, None)
  } yield op

  /** Experimental flatMap API for EtlJob, don't use in production
    */
  def flatMap[R1, OP1](fn: OP => EtlTask[R1, OP1]): EtlJob[R with R1, OP1] = EtlJob.flatMap[R, OP, R1, OP1](this, fn)

  /** Experimental *>(variant of flatMap that ignores the value produced by this effect) API for EtlJob, don't use in production
    */
  def *>[R1, OP1](that: EtlTask[R1, OP1]): EtlJob[R with R1, OP1] = EtlJob.flatMap[R, OP, R1, OP1](this, _ => that)
}

object EtlJob {

  /** Experimental flatMap API for EtlJob, don't use in production
    */
  def flatMap[R1, OP1, R2, OP2](currentJob: EtlJob[R1, OP1], fn: OP1 => EtlTask[R2, OP2]): EtlJob[R1 with R2, OP2] =
    new EtlJob[R1 with R2, OP2] {
      override protected def process: RIO[R1 with R2 with Audit, OP2] = currentJob.process.flatMap(op => fn(op).execute)
    }
}
