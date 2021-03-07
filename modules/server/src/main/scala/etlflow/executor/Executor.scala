package etlflow.executor

import caliban.CalibanError.ExecutionError
import doobie.hikari.HikariTransactor
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.gcp.{DP, DPService}
import etlflow.utils.EtlFlowHelper._
import etlflow.utils.Executor._
import etlflow.utils.JsonJackson.convertToJson
import etlflow.utils.db.{Query, Update}
import etlflow.utils.{Config, JDBC, UtilityFunctions => UF}
import etlflow.{EtlJobPropsMapping, EtlJobProps}
import org.slf4j.{Logger, LoggerFactory}
import zio._
import zio.blocking.{Blocking, blocking}
import zio.clock.Clock
import zio.duration.{Duration => ZDuration}
import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag

trait Executor extends K8SExecutor with EtlJobValidator  with etlflow.utils.EtlFlowUtils {
  lazy val executor_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  final def runActiveEtlJob[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](
                                                                                             args: EtlJobArgs,
                                                                                             transactor: HikariTransactor[Task],
                                                                                             sem: Semaphore,
                                                                                             config: Config,
                                                                                             etl_job_name_package: String,
                                                                                             submittedFrom:String,
                                                                                             jobQueue: Queue[(String,String,String,String)]
                                                                                           ): Task[Option[EtlJob]] = {
    for {
      _       <- UIO(executor_logger.info(s"Checking if job  ${args.name} is active at ${UF.getCurrentTimestampAsString()}"))
      defualt_props  = getJobActualProps[EJN](args.name,etl_job_name_package)
      actual_props   = args.props.map(x => (x.key,x.value)).toMap
      final_props    =  defualt_props ++ actual_props + ("submitted_at" -> UF.getCurrentTimestampAsString())
      _       <- UIO(executor_logger.info("job_retry_delay_in_minutes" + defualt_props("job_retry_delay_in_minutes")))
      _       <- UIO(executor_logger.info("job_retries" + defualt_props("job_retries")))

      _       <- jobQueue.offer((args.name.take(25),submittedFrom,convertToJson(final_props.filter(x => x._2 != null && x._2.trim != "")),UF.getCurrentTimestampAsString()))
      etljob  <- Query.getCronJobFromDB(args.name,transactor).flatMap( cj =>
        if (cj.is_active) {
          UIO(executor_logger.info(s"Running job ${cj.job_name} with schedule ${cj.schedule} at ${UF.getCurrentTimestampAsString()}")) *> runEtlJob[EJN](args, transactor, sem, config, etl_job_name_package,final_props("job_retry_delay_in_minutes").toInt,final_props("job_retries").toInt).map(Some(_))
        } else
          UIO(executor_logger.info(s"Skipping inactive cron job ${cj.job_name} with schedule ${cj.schedule} at ${UF.getCurrentTimestampAsString()}")).as(None)
      )
    } yield etljob
  }

  final def runEtlJob[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](
                                                                                       args: EtlJobArgs,
                                                                                       transactor: HikariTransactor[Task],
                                                                                       sem: Semaphore,
                                                                                       config: Config,
                                                                                       etl_job_name_package: String,
                                                                                       spaced:Int,
                                                                                       retry:Int
                                                                                     ): Task[EtlJob] = {
    UF.getEtlJobName[EJN](args.name,etl_job_name_package).getActualProperties(Map.empty).job_deploy_mode match {
      case lsp @ LOCAL_SUBPROCESS(script_path, heap_min_memory, heap_max_memory) =>
        runLocalSubProcessJob(args, transactor, etl_job_name_package, lsp, sem,true,spaced,retry)
      case LOCAL =>
        runLocalJob(args, transactor, etl_job_name_package, sem,true,spaced,retry)
      case dp @ DATAPROC(project, region, endpoint, cluster_name) =>
        runDataProcJob(args, transactor, etl_job_name_package, dp, config.dataProc.map(_.mainClass).getOrElse(""), config.dataProc.map(_.depLibs).getOrElse(List.empty), sem,true,spaced,retry)
      case LIVY(_) =>
        Task.fail(ExecutionError("Deploy mode livy not yet supported"))
      case k8s @ KUBERNETES(imageName, nameSpace, envVar, containerName, entryPoint, restartPolicy) =>
        runKubernetesJob(args, config.dbLog, transactor, etl_job_name_package, k8s, sem,true,spaced,retry)
    }
  }

  def runLocalJob(args: EtlJobArgs, transactor: HikariTransactor[Task], etl_job_name_package: String, sem: Semaphore, fork: Boolean = true,spaced:Int=0, retry:Int=0): Task[EtlJob] = {
    var retry_number = 0
    for {
      _       <- UIO(executor_logger.info("job_retry_delay_in_minutes" +spaced))
      _       <- UIO(executor_logger.info("job_retries" + retry))
      etlJob     <- validateJob(args, etl_job_name_package)
      props_map  = args.props.map(x => (x.key,x.value)).toMap
      jobRun     = blocking(LocalExecutorService.executeLocalJob(args.name, props_map,etl_job_name_package).provideLayer(LocalExecutor.live)).provideLayer(Blocking.live).map(x => (retry_number += 1))
        .retry(Schedule.spaced(ZDuration.fromScala(Duration(spaced,MINUTES))) && Schedule.recurs(retry)).provideLayer(Clock.live).tapError( ex => UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor)
      ) *> Update.updateSuccessJob(args.name, transactor)
      _          <- if(fork) sem.withPermit(jobRun).forkDaemon else sem.withPermit(jobRun)
      _       <- UIO(executor_logger.info("inc" + retry_number))
    } yield etlJob
  }
  def runDataProcJob(args: EtlJobArgs, transactor: HikariTransactor[Task], etl_job_name_package: String, config: DATAPROC, main_class: String, dp_libs: List[String], sem: Semaphore, fork: Boolean = true,spaced:Int=0, retry:Int=0): Task[EtlJob] = {
    for {
      etlJob    <- validateJob(args, etl_job_name_package)
      props_map = args.props.map(x => (x.key, x.value)).toMap
      jobRun    = blocking(DPService.executeSparkJob(args.name, props_map, main_class, dp_libs).provideLayer(DP.live(config))).provideLayer(Blocking.live)
        .retry(Schedule.spaced(ZDuration.fromScala(Duration(spaced,MINUTES))) && Schedule.recurs(retry)).provideLayer(Clock.live).tapError( ex => UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor)
      ) *> Update.updateSuccessJob(args.name, transactor)
      _          <- if(fork) sem.withPermit(jobRun).forkDaemon else sem.withPermit(jobRun)
    } yield etlJob
  }
  def runLocalSubProcessJob(args: EtlJobArgs, transactor: HikariTransactor[Task], etl_job_name_package: String, config: LOCAL_SUBPROCESS, sem: Semaphore, fork: Boolean = true,spaced:Int=0, retry:Int=0): Task[EtlJob] = {
    for {
      etlJob    <- validateJob(args, etl_job_name_package)
      props_map = args.props.map(x => (x.key, x.value)).toMap
      jobRun    = blocking(LocalExecutorService.executeLocalSubProcessJob(args.name, props_map, config).provideLayer(LocalExecutor.live)).provideLayer(Blocking.live)
        .retry(Schedule.spaced(ZDuration.fromScala(Duration(spaced,MINUTES))) && Schedule.recurs(retry)).provideLayer(Clock.live).tapError( ex =>
        UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor)
      ) *> Update.updateSuccessJob(args.name, transactor)
      _          <- if(fork) sem.withPermit(jobRun).forkDaemon else sem.withPermit(jobRun)
    } yield etlJob
  }
  def runKubernetesJob(args: EtlJobArgs, db: JDBC, transactor: HikariTransactor[Task], etl_job_name_package: String, config: KUBERNETES, sem: Semaphore, fork: Boolean = true,spaced:Int=0, retry:Int=0): Task[EtlJob] = {
    for {
      etlJob  <- validateJob(args, etl_job_name_package)
      jobRun  = blocking(runK8sJob(args,db,config)).provideLayer(Blocking.live).retry(Schedule.spaced(ZDuration.fromScala(Duration(spaced,MINUTES))) && Schedule.recurs(retry)).provideLayer(Clock.live).tapError( ex =>
        UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor)
      ) *> Update.updateSuccessJob(args.name, transactor)
      _        <- if(fork) sem.withPermit(jobRun).forkDaemon else sem.withPermit(jobRun)
    } yield etlJob
  }

  def runEtlJobsFromApi[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](args: EtlJobArgs,transactor: HikariTransactor[Task],sem: Semaphore,config: Config, etl_job_name_package: String,jobQueue: Queue[(String,String,String,String)]): Task[Option[EtlJob]] ={
    runActiveEtlJob[EJN](args,transactor,sem,config,etl_job_name_package,"Rest-Api",jobQueue)
  }
}