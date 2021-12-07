package etlflow.etljobs

import etlflow.core.CoreLogEnv
import etlflow.etlsteps._
import etlflow.EtlJobProps
import zio.{Task, ZIO}

trait SequentialEtlJob[EJP <: EtlJobProps] extends GenericEtlJob[EJP] {

  def etlStepList: List[EtlStep[Unit,Unit]]
  override val job_type: String =  "SequentialEtlJob"

  final override val job: ZIO[CoreLogEnv, Throwable, Unit] = for {
    step_list <- Task.succeed(etlStepList.map(_.execute(())))
    job       <- ZIO.collectAll(step_list).unit
  } yield job
}
