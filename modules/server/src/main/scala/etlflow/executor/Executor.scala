package etlflow.executor

import caliban.CalibanError.ExecutionError
import doobie.hikari.HikariTransactor
import etlflow.Credential.JDBC
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.gcp.{DP, DPService}
import etlflow.utils.EtlFlowHelper._
import etlflow.utils.Executor._
import etlflow.utils.JsonJackson.convertToJson
import etlflow.utils.db.{Query, Update}
import etlflow.utils.{Config, UtilityFunctions => UF}
import etlflow.{EtlJobProps, EtlJobPropsMapping}
import zio._
import zio.blocking.{Blocking, blocking}
import zio.clock.Clock
import zio.duration.{Duration => ZDuration}
import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag

trait Executor extends K8SExecutor with EtlJobValidator with etlflow.utils.EtlFlowUtils {

  final def runActiveEtlJob[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag]
  (args: EtlJobArgs, transactor: HikariTransactor[Task], sem: Semaphore, config: Config, etl_job_name_package: String, submitted_from: String, job_queue: Queue[(String,String,String,String)]): RIO[Blocking with Clock, EtlJob] = {
    for {
      default_props  <- Task(getJobPropsMapping[EJN](args.name,etl_job_name_package)).mapError(e => ExecutionError(e.getMessage))
      actual_props   =  args.props.map(x => (x.key,x.value)).toMap
      _              <- UIO(logger.info(s"Checking if job ${args.name} is active at ${UF.getCurrentTimestampAsString()}"))
      db_job         <- Query.getJob(args.name,transactor)
      final_props    =  default_props ++ actual_props + ("job_status" -> (if (db_job.is_active) "ACTIVE" else "INACTIVE"))
      props_json     = convertToJson(final_props.filter(x => x._2 != null && x._2.trim != ""))
      _              <- job_queue.offer((args.name,submitted_from,props_json,UF.getCurrentTimestampAsString()))
      _              <- if (db_job.is_active) UIO(logger.info(s"Submitting job ${db_job.job_name} from $submitted_from at ${UF.getCurrentTimestampAsString()}")) *> runEtlJob[EJN](args, transactor, sem, config, etl_job_name_package, default_props("job_retry_delay_in_minutes").toInt, default_props("job_retries").toInt)
                        else UIO(logger.info(s"Skipping inactive job ${db_job.job_name} submitted from $submitted_from at ${UF.getCurrentTimestampAsString()}")) *> ZIO.fail(ExecutionError(s"Job ${db_job.job_name} is disabled"))
    } yield EtlJob(args.name,final_props)
  }

  final def runEtlJob[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag]
  (args: EtlJobArgs, transactor: HikariTransactor[Task], sem: Semaphore, config: Config, etl_job_name_package: String, spaced:Int, retry:Int): RIO[Blocking with Clock, Unit] = {
    UF.getEtlJobName[EJN](args.name,etl_job_name_package).job_deploy_mode match {
      case lsp @ LOCAL_SUBPROCESS(_, _, _) =>
        runLocalSubProcessJob(args, transactor, etl_job_name_package, lsp, sem,true,spaced,retry)
      case LOCAL =>
        runLocalJob(args, transactor, etl_job_name_package, sem,true,spaced,retry)
      case dp @ DATAPROC(_, _, _, _) =>
        runDataProcJob(args, transactor, etl_job_name_package, dp, config.dataProc.map(_.mainClass).getOrElse(""), config.dataProc.map(_.depLibs).getOrElse(List.empty), sem,true,spaced,retry)
      case LIVY(_) =>
        Task.fail(ExecutionError("Deploy mode livy not yet supported"))
      case k8s @ KUBERNETES(_, _, _, _, _, _) =>
        runKubernetesJob(args, config.dbLog, transactor, etl_job_name_package, k8s, sem,true,spaced,retry)
    }
  }

  final def runLocalJob(args: EtlJobArgs, transactor: HikariTransactor[Task], etl_job_name_package: String, sem: Semaphore, fork: Boolean = true,spaced:Int=0, retry:Int=0): RIO[Blocking with Clock,Unit] = {
    val jobRun = blocking(LocalExecutorService.executeLocalJob(args.name, args.props.map(x => (x.key, x.value)).toMap,etl_job_name_package).provideLayer(LocalExecutor.live))
      .retry(Schedule.spaced(ZDuration.fromScala(Duration(spaced,MINUTES))) && Schedule.recurs(retry))
      .tapError( ex =>
        UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor)
      ) *> Update.updateSuccessJob(args.name, transactor)
    (if(fork) sem.withPermit(jobRun).forkDaemon else sem.withPermit(jobRun)).as(())
  }
  final def runDataProcJob(args: EtlJobArgs, transactor: HikariTransactor[Task], etl_job_name_package: String, config: DATAPROC, main_class: String, dp_libs: List[String], sem: Semaphore, fork: Boolean = true,spaced:Int=0, retry:Int=0): RIO[Blocking with Clock, Unit] = {
    val jobRun = blocking(DPService.executeSparkJob(args.name, args.props.map(x => (x.key, x.value)).toMap, main_class, dp_libs).provideLayer(DP.live(config)))
      .retry(Schedule.spaced(ZDuration.fromScala(Duration(spaced,MINUTES))) && Schedule.recurs(retry))
      .tapError( ex =>
        UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor)
      ) *> Update.updateSuccessJob(args.name, transactor)
    (if(fork) sem.withPermit(jobRun).forkDaemon else sem.withPermit(jobRun)).as(())
  }
  final def runLocalSubProcessJob(args: EtlJobArgs, transactor: HikariTransactor[Task], etl_job_name_package: String, config: LOCAL_SUBPROCESS, sem: Semaphore, fork: Boolean = true,spaced:Int=0, retry:Int=0): RIO[Blocking with Clock, Unit] = {
    val jobRun = blocking(LocalExecutorService.executeLocalSubProcessJob(args.name, args.props.map(x => (x.key, x.value)).toMap, config).provideLayer(LocalExecutor.live))
      .retry(Schedule.spaced(ZDuration.fromScala(Duration(spaced,MINUTES))) && Schedule.recurs(retry))
      .tapError( ex =>
        UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor)
      ) *> Update.updateSuccessJob(args.name, transactor)
    (if(fork) sem.withPermit(jobRun).forkDaemon else sem.withPermit(jobRun)).as(())
  }
  final def runKubernetesJob(args: EtlJobArgs, db: JDBC, transactor: HikariTransactor[Task], etl_job_name_package: String, config: KUBERNETES, sem: Semaphore, fork: Boolean = true,spaced:Int=0, retry:Int=0): RIO[Blocking with Clock, Unit] = {
    val jobRun  = blocking(runK8sJob(args,db,config))
      .retry(Schedule.spaced(ZDuration.fromScala(Duration(spaced,MINUTES))) && Schedule.recurs(retry))
      .tapError( ex =>
        UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor)
      ) *> Update.updateSuccessJob(args.name, transactor)
    (if(fork) sem.withPermit(jobRun).forkDaemon else sem.withPermit(jobRun)).as(())
  }
}