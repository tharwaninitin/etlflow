package etlflow.executor

import caliban.CalibanError.ExecutionError
import etlflow.api.ExecutorTask
import etlflow.api.Schema._
import etlflow.common.DateTimeFunctions.{getCurrentTimestamp, getCurrentTimestampAsString}
import etlflow.gcp.{DP, DPService}
import etlflow.jdbc.{DB, EtlJob}
import etlflow.log.ApplicationLogger
import etlflow.schema.Config
import etlflow.utils.Executor._
import etlflow.utils.JsonJackson.convertToJson
import etlflow.utils.{CacheHelper, EtlFlowUtils, UtilityFunctions => UF}
import etlflow.{EJPMType, JobEnv}
import scalacache.caffeine.CaffeineCache
import zio._
import zio.blocking.blocking
import zio.duration.{Duration => ZDuration}

import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag

case class Executor[EJN <: EJPMType : TypeTag](sem: Map[String, Semaphore], config: Config, ejpm_package: String, cache: CaffeineCache[QueueDetails])
  extends EtlFlowUtils with ApplicationLogger {

  final def runActiveEtlJob(args: EtlJobArgs, submitted_from: String, fork: Boolean = true): ExecutorTask[EtlJob] = {
    for {
      mapping_props  <- Task(getJobPropsMapping[EJN](args.name,ejpm_package)).mapError(e => ExecutionError(e.getMessage))
      job_props      =  args.props.getOrElse(List.empty).map(x => (x.key,x.value)).toMap
      _              <- UIO(logger.info(s"Checking if job ${args.name} is active at ${getCurrentTimestampAsString()}"))
      db_job         <- DB.getJob(args.name)
      final_props    =  mapping_props ++ job_props + ("job_status" -> (if (db_job.is_active) "ACTIVE" else "INACTIVE"))
      props_json     = convertToJson(final_props.filter(x => x._2 != null && x._2.trim != ""))
      retry          = mapping_props.getOrElse("job_retries","0").toInt
      spaced         = mapping_props.getOrElse("job_retry_delay_in_minutes","0").toInt
      _              <- if (db_job.is_active)
                        for {
                          _     <- UIO(logger.info(s"Submitting job ${db_job.job_name} from $submitted_from at ${getCurrentTimestampAsString()}"))
                          key   = s"${args.name} ${getCurrentTimestamp}"
                          value = QueueDetails(args.name, props_json, submitted_from, getCurrentTimestampAsString())
                          _     = CacheHelper.putKey(cache, key, value)
                          _ <- runEtlJob(args, key, retry, spaced, fork)
                        } yield ()
                        else UIO(logger.info(s"Skipping inactive job ${db_job.job_name} submitted from $submitted_from at ${getCurrentTimestampAsString()}")) *> ZIO.fail(ExecutionError(s"Job ${db_job.job_name} is disabled"))
    } yield EtlJob(args.name,final_props)
  }

  private def runEtlJob(args: EtlJobArgs, cache_key: String, retry: Int = 0, spaced: Int = 0, fork: Boolean = true): ExecutorTask[Unit] = {
    val actual_props = args.props.getOrElse(List.empty).map(x => (x.key,x.value)).toMap

    val jobRun: RIO[JobEnv,Unit] = UF.getEtlJobName[EJN](args.name,ejpm_package).job_deploy_mode match {
      case lsp @ LOCAL_SUBPROCESS(_, _, _) =>
        LocalSubProcessExecutor(lsp).executeJob(args.name, actual_props)
      case LOCAL =>
        LocalExecutor(ejpm_package).executeJob(args.name, actual_props)
      case dp @ DATAPROC(_, _, _, _) =>
        val main_class = config.dataProc.map(_.mainClass).getOrElse("")
        val dp_libs = config.dataProc.map(_.depLibs).getOrElse(List.empty)
        DPService.executeSparkJob(args.name, actual_props, main_class, dp_libs).provideLayer(DP.live(dp))
      case LIVY(_) =>
        Task.fail(ExecutionError("Deploy mode livy not yet supported"))
      case KUBERNETES(_, _, _, _, _, _) =>
        Task.fail(ExecutionError("Deploy mode KUBERNETES not yet supported"))
    }

    val loggedJobRun: ExecutorTask[Long] = jobRun
      .ensuring(UIO(CacheHelper.removeKey(cache, cache_key)))
      .retry(Schedule.spaced(ZDuration.fromScala(Duration(spaced,MINUTES))) && Schedule.recurs(retry))
      .tapError( ex =>
        UIO(logger.error(ex.getMessage)) *> DB.updateFailedJob(args.name, getCurrentTimestamp)
      ) *> DB.updateSuccessJob(args.name, getCurrentTimestamp)

    blocking(if (fork) sem(args.name).withPermit(loggedJobRun).forkDaemon else sem(args.name).withPermit(loggedJobRun)).unit
  }
}