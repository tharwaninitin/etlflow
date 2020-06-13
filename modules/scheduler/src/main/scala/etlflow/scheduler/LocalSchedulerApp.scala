package etlflow.scheduler

import caliban.CalibanError.ExecutionError
import doobie.hikari.HikariTransactor
import etlflow.scheduler.EtlFlowHelper._
import etlflow.utils.{GlobalProperties, UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import zio._
import etlflow.etljobs.{EtlJob => EtlFlowEtlJob}
import scala.reflect.runtime.universe.TypeTag

abstract class LocalSchedulerApp[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag, EJGP <: GlobalProperties : TypeTag]
  extends SchedulerApp[EJN,EJP,EJGP] {

  def toEtlJob(job_name: EJN): (EJP,Option[EJGP]) => EtlFlowEtlJob

  final override def runEtlJob(args: EtlJobArgs, transactor: HikariTransactor[Task]): Task[EtlJob] = {

    val etlJobDetails: Task[(EJN, EtlFlowEtlJob, Map[String, String])] = Task {
      val job_name      = UF.getEtlJobName[EJN](args.name, etl_job_name_package)
      val props_map     = args.props.map(x => (x.key,x.value)).toMap
      val etl_job       = toEtlJob(job_name)(job_name.getActualProperties(props_map),globalProperties)
      etl_job.job_name  = job_name.toString
      (job_name,etl_job,props_map)
    }.mapError(e => ExecutionError(e.getMessage))

    for {
      (job_name,etl_job,props_map) <- etlJobDetails
      execution_props   <- Task {
                             UF.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
                               .map(x => (x._1, x._2.toString))
                           }.mapError{ e =>
                             logger.error(e.getMessage)
                             ExecutionError(e.getMessage)
                           }
      _                  <- etl_job.execute().foldM(
                              ex => updateFailedJob(job_name.toString,transactor),
                              _  => updateSuccessJob(job_name.toString,transactor)
                            ).forkDaemon
    } yield EtlJob(args.name,execution_props)
  }
}
