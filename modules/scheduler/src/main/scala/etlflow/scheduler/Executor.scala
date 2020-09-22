package etlflow.scheduler

import doobie.hikari.HikariTransactor
import etlflow.executor.ExecutorHelper
import etlflow.scheduler.api.EtlFlowHelper._
import etlflow.scheduler.db.Update
import etlflow.utils.Executor.{DATAPROC, KUBERNETES, LOCAL_SUBPROCESS}
import etlflow.{EtlJobName, EtlJobProps}
import etlflow.etljobs.{EtlJob => EtlFlowEtlJob}
import etlflow.utils.{Config, UtilityFunctions => UF}
import zio._
import zio.blocking.blocking
import scala.reflect.runtime.universe.TypeTag

abstract class Executor[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag] extends WebServer[EJN,EJP] with ExecutorHelper {

  val main_class: String
  val dp_libs: List[String]

  def toEtlJob(job_name: EJN): (EJP,Config) => EtlFlowEtlJob

  final override def runEtlJobLocal(args: EtlJobArgs, transactor: HikariTransactor[Task], sem: Semaphore): Task[EtlJob] = {
    for {
      etlJob     <- validateJob(args, etl_job_name_package)
      job_name   = UF.getEtlJobName[EJN](etlJob.name, etl_job_name_package)
      props_map  = args.props.map(x => (x.key,x.value)).toMap
      etl_job    = toEtlJob(job_name)(job_name.getActualProperties(props_map),app_config)
      jobRun     = blocking(etl_job.execute()).provideLayer(ZEnv.live).foldM(
                    ex => UIO(println(ex.getMessage)) *> Update.updateFailedJob(job_name.toString,transactor),
                    _  => Update.updateSuccessJob(job_name.toString,transactor)
                  )
      _           <- sem.withPermit(jobRun).forkDaemon
    } yield etlJob
  }

  final override def runEtlJobLocalSubProcess(args: EtlJobArgs, transactor: HikariTransactor[Task], config: LOCAL_SUBPROCESS, sem: Semaphore): Task[EtlJob] = {
    runLocalSubProcessJob(args, transactor, etl_job_name_package, config, sem)
  }

  final override def runEtlJobDataProc(args: EtlJobArgs, transactor: HikariTransactor[Task], config: DATAPROC, sem: Semaphore): Task[EtlJob] = {
    runDataprocJob(args, transactor, config, main_class, dp_libs, etl_job_name_package, sem)
  }

  final override def runEtlJobKubernetes(args: EtlJobArgs, transactor: HikariTransactor[Task], config: KUBERNETES, sem: Semaphore): Task[EtlJob] = {
    runKubernetesJob(args, app_config.dbLog, transactor, etl_job_name_package, config, sem)
  }
}