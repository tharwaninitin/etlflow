package etlflow.scheduler

import doobie.hikari.HikariTransactor
import etlflow.etljobs.{EtlJob => EtlFlowEtlJob}
import etlflow.scheduler.api.EtlFlowHelper._
import etlflow.scheduler.db.Update
import etlflow.executor.ExecutorHelper
import etlflow.utils.Executor.{DATAPROC, KUBERNETES, LOCAL_SUBPROCESS}
import etlflow.utils.{Config, UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import zio._
import zio.blocking.blocking
import scala.reflect.runtime.universe.TypeTag

abstract class Executor[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag] extends WebServer[EJN,EJP] with ExecutorHelper {

  val main_class: String
  val dp_libs: List[String]

  def toEtlJob(job_name: EJN): (EJP,Config) => EtlFlowEtlJob

  final override def runEtlJobLocal(args: EtlJobArgs, transactor: HikariTransactor[Task]): Task[EtlJob] = {
    for {
      etlJob     <- validateJob(args, etl_job_name_package)
      job_name   = UF.getEtlJobName[EJN](etlJob.name, etl_job_name_package)
      props_map  = args.props.map(x => (x.key,x.value)).toMap
      etl_job    = toEtlJob(job_name)(job_name.getActualProperties(props_map),app_config)
      _ <- blocking(etl_job.execute()).provideLayer(ZEnv.live).foldM(
        ex => UIO(println(ex.getMessage)) *> Update.updateFailedJob(job_name.toString,transactor),
        _  => Update.updateSuccessJob(job_name.toString,transactor)
      ).forkDaemon
    } yield etlJob
  }

  final override def runEtlJobLocalSubProcess(args: EtlJobArgs, transactor: HikariTransactor[Task],config: LOCAL_SUBPROCESS): Task[EtlJob] = {
    runLocalSubProcessJob(args, transactor, etl_job_name_package, config)
  }

  final override def runEtlJobDataProc(args: EtlJobArgs, transactor: HikariTransactor[Task], config: DATAPROC): Task[EtlJob] = {
    runDataprocJob(args, transactor, config, main_class, dp_libs, etl_job_name_package)
  }

  final override def runEtlJobKubernetes(args: EtlJobArgs, transactor: HikariTransactor[Task], config: KUBERNETES): Task[EtlJob] = {
    runKubernetesJob(args, app_config.dbLog, transactor, etl_job_name_package, config)
  }
}