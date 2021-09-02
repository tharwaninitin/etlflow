package etlflow.executor

import caliban.CalibanError.ExecutionError
import etlflow.api.Schema._
import etlflow.cache.{Cache, CacheApi, CacheEnv}
import etlflow.db.{DBApi, EtlJob}
import etlflow.gcp.{DP, DPService}
import etlflow.json.JsonApi
import etlflow.schema.Config
import etlflow.schema.Executor._
import etlflow.utils.DateTimeApi.{getCurrentTimestamp, getCurrentTimestampAsString}
import etlflow.utils.{ApplicationLogger, ReflectAPI => RF}
import etlflow.{CoreEnv, EJPMType, schema}
import zio._
import zio.blocking.blocking
import zio.duration.{Duration => ZDuration}
import scala.concurrent.duration._

case class Executor[T <: EJPMType : Tag](sem: Map[String, Semaphore], config: Config, cache: Cache[QueueDetails])
  extends ApplicationLogger {

  final def runActiveEtlJob(args: EtlJobArgs, submitted_from: String, fork: Boolean = true): RIO[CoreEnv with CacheEnv, EtlJob] = {
    for {
      ejpm           <- RF.getJob[T](args.name).mapError(e => ExecutionError(e.getMessage))
      mapping_props  = ejpm.getProps
      job_props      =  args.props.getOrElse(List.empty).map(x => (x.key,x.value)).toMap
      _              <- UIO(logger.info(s"Checking if job ${args.name} is active at ${getCurrentTimestampAsString()}"))
      db_job         <- DBApi.getJob(args.name)
      final_props    =  mapping_props ++ job_props + ("job_status" -> (if (db_job.is_active) "ACTIVE" else "INACTIVE"))
      props_json     <- JsonApi.convertToString(final_props.filter(x => x._2 != null && x._2.trim != ""), List.empty)
      retry          = mapping_props.getOrElse("job_retries","0").toInt
      spaced         = mapping_props.getOrElse("job_retry_delay_in_minutes","0").toInt
      _              <- if (db_job.is_active)
                        for {
                          _     <- UIO(logger.info(s"Submitting job ${db_job.job_name} from $submitted_from at ${getCurrentTimestampAsString()}"))
                          key   = s"${args.name} ${getCurrentTimestamp}"
                          value = QueueDetails(args.name, props_json, submitted_from, getCurrentTimestampAsString())
                          _     <- CacheApi.put(cache, key, value)
                          _     <- runEtlJob(args, ejpm.job_deploy_mode, key, retry, spaced, fork)
                        } yield ()
                        else UIO(logger.info(s"Skipping inactive job ${db_job.job_name} submitted from $submitted_from at ${getCurrentTimestampAsString()}")) *> ZIO.fail(ExecutionError(s"Job ${db_job.job_name} is disabled"))
    } yield EtlJob(args.name,final_props)
  }

  private def runEtlJob(args: EtlJobArgs, deploy_mode: schema.Executor, cache_key: String, retry: Int = 0, spaced: Int = 0, fork: Boolean = true): RIO[CoreEnv with CacheEnv, Unit] = {
    val actual_props = args.props.getOrElse(List.empty).map(x => (x.key,x.value)).toMap

    val jobRun: RIO[CoreEnv,Unit] =
      deploy_mode
         match {
          case lsp @ LOCAL_SUBPROCESS(_, _, _) =>
            LocalSubProcessExecutor(lsp).executeJob(args.name, actual_props)
          case LOCAL =>
            LocalExecutor[T]().executeJob(args.name, actual_props, config.slack)
          case dp @ DATAPROC(_, _, _, _, _) =>
            val main_class = config.dataproc.map(_.mainclass).getOrElse("")
            val dp_libs = config.dataproc.map(_.deplibs).getOrElse(List.empty)
            DPService.executeSparkJob(args.name, actual_props, main_class, dp_libs).provideLayer(DP.live(dp))
          case LIVY(_) =>
            Task.fail(ExecutionError("Deploy mode livy not yet supported"))
          case KUBERNETES(_, _, _, _, _, _) =>
            Task.fail(ExecutionError("Deploy mode KUBERNETES not yet supported"))
        }

    val loggedJobRun: RIO[CoreEnv with CacheEnv, Long] = jobRun
      .ensuring(CacheApi.remove(cache, cache_key).orDie)
      .retry(Schedule.spaced(ZDuration.fromScala(Duration(spaced,MINUTES))) && Schedule.recurs(retry))
      .tapError( ex =>
        UIO(logger.error(ex.getMessage)) *> DBApi.updateFailedJob(args.name, getCurrentTimestamp)
      ) *> DBApi.updateSuccessJob(args.name, getCurrentTimestamp)

    blocking(if (fork) sem(args.name).withPermit(loggedJobRun).forkDaemon else sem(args.name).withPermit(loggedJobRun)).unit
  }
}