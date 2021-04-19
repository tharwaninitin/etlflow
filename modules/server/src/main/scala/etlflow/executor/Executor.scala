package etlflow.executor

import caliban.CalibanError.ExecutionError
import etlflow.utils.EtlFlowUtils
import etlflow.gcp.{DP, DPService}
import etlflow.jdbc.{DB, DBServerEnv}
import etlflow.api.Schema._
import etlflow.api.ServerJobEnv
import etlflow.utils.Executor._
import etlflow.utils.JsonJackson.convertToJson
import etlflow.utils.{Config, UtilityFunctions => UF}
import etlflow.{DBEnv, EJPMType, JobEnv}
import zio._
import zio.blocking.{Blocking, blocking}
import zio.clock.Clock
import zio.duration.{Duration => ZDuration}

import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag

trait Executor extends K8SExecutor with EtlFlowUtils {

  final def runActiveEtlJob[EJN <: EJPMType : TypeTag](args: EtlJobArgs, sem: Semaphore, config: Config, etl_job_name_package: String, submitted_from: String, job_queue: Queue[(String,String,String,String)], fork: Boolean = true)
  : RIO[ServerJobEnv, EtlJob] = {
    for {
      mapping_props  <- Task(getJobPropsMapping[EJN](args.name,etl_job_name_package)).mapError(e => ExecutionError(e.getMessage))
      job_props      =  args.props.getOrElse(List.empty).map(x => (x.key,x.value)).toMap
      _              <- UIO(logger.info(s"Checking if job ${args.name} is active at ${UF.getCurrentTimestampAsString()}"))
      db_job         <- DB.getJob(args.name)
      final_props    =  mapping_props ++ job_props + ("job_status" -> (if (db_job.is_active) "ACTIVE" else "INACTIVE"))
      props_json     = convertToJson(final_props.filter(x => x._2 != null && x._2.trim != ""))
      _              <- job_queue.offer((args.name,submitted_from,props_json,UF.getCurrentTimestampAsString()))
      retry          = mapping_props.getOrElse("job_retries","0").toInt
      spaced         = mapping_props.getOrElse("job_retry_delay_in_minutes","0").toInt
      _              <- if (db_job.is_active) UIO(logger.info(s"Submitting job ${db_job.job_name} from $submitted_from at ${UF.getCurrentTimestampAsString()}")) *> runEtlJob[EJN](args, sem, config, etl_job_name_package, retry, spaced, fork)
                        else UIO(logger.info(s"Skipping inactive job ${db_job.job_name} submitted from $submitted_from at ${UF.getCurrentTimestampAsString()}")) *> ZIO.fail(ExecutionError(s"Job ${db_job.job_name} is disabled"))
    } yield EtlJob(args.name,final_props)
  }

  final def runEtlJob[EJN <: EJPMType : TypeTag](args: EtlJobArgs, sem: Semaphore, config: Config, etl_job_name_package: String, retry: Int = 0, spaced: Int = 0, fork: Boolean = true)
  : RIO[ServerJobEnv, Unit] = {
    val actual_props = args.props.getOrElse(List.empty).map(x => (x.key,x.value)).toMap

    val jobRun: RIO[JobEnv,Unit] = UF.getEtlJobName[EJN](args.name,etl_job_name_package).job_deploy_mode match {
      case lsp @ LOCAL_SUBPROCESS(_, _, _) =>
        LocalSubProcessExecutor(lsp).executeJob(args.name, actual_props)
      case LOCAL =>
        LocalExecutor(etl_job_name_package).executeJob(args.name, actual_props)
      case dp @ DATAPROC(_, _, _, _) =>
        val main_class = config.dataProc.map(_.mainClass).getOrElse("")
        val dp_libs = config.dataProc.map(_.depLibs).getOrElse(List.empty)
        DPService.executeSparkJob(args.name, actual_props, main_class, dp_libs).provideLayer(DP.live(dp))
      case LIVY(_) =>
        Task.fail(ExecutionError("Deploy mode livy not yet supported"))
      case k8s @ KUBERNETES(_, _, _, _, _, _) =>
        runK8sJob(args, config.dbLog, k8s).unit
    }

    val loggedJobRun: RIO[ServerJobEnv, Long] = blocking(jobRun)
      .retry(Schedule.spaced(ZDuration.fromScala(Duration(spaced,MINUTES))) && Schedule.recurs(retry))
      .tapError( ex =>
        UIO(logger.error(ex.getMessage)) *> DB.updateFailedJob(args.name, UF.getCurrentTimestamp)
      ) *> DB.updateSuccessJob(args.name, UF.getCurrentTimestamp)

    (if (fork) sem.withPermit(loggedJobRun).forkDaemon else sem.withPermit(loggedJobRun)).unit
  }
}