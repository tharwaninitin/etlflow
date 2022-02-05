package etlflow.executor

import caliban.CalibanError.ExecutionError
import etlflow.db.DBServerEnv
import gcp4zio.{DPJob, DPJobApi}
import etlflow.json.JsonEnv
import etlflow.model.Executor
import etlflow.model.Config
import etlflow.model.Executor._
import etlflow.server.DBServerApi
import etlflow.server.model.{EtlJob, EtlJobArgs}
import etlflow.utils.DateTimeApi.{getCurrentTimestamp, getCurrentTimestampAsString}
import etlflow.utils.{ApplicationLogger, ReflectAPI => RF}
import etlflow.EJPMType
import zio._
import zio.blocking.blocking
import zio.duration.{Duration => ZDuration}

import scala.concurrent.duration._

case class ServerExecutor[T <: EJPMType: Tag](sem: Map[String, Semaphore], config: Config) extends ApplicationLogger {

  final def runActiveEtlJob(
      args: EtlJobArgs,
      submitted_from: String,
      fork: Boolean = true
  ): RIO[ZEnv with JsonEnv with DBServerEnv, EtlJob] =
    for {
      ejpm <- RF.getJob[T](args.name).mapError(e => ExecutionError(e.getMessage))
      mapping_props = ejpm.getProps
      job_props     = args.props.getOrElse(List.empty).map(x => (x.key, x.value)).toMap
      _      <- UIO(logger.info(s"Checking if job ${args.name} is active at ${getCurrentTimestampAsString()}"))
      db_job <- DBServerApi.getJob(args.name)
      final_props = mapping_props ++ job_props + ("job_status" -> (if (db_job.is_active) "ACTIVE" else "INACTIVE"))
      retry       = mapping_props.getOrElse("job_retries", "0").toInt
      spaced      = mapping_props.getOrElse("job_retry_delay_in_minutes", "0").toLong
      _ <-
        if (db_job.is_active)
          for {
            _ <- UIO(
              logger.info(s"Submitting job ${db_job.job_name} from $submitted_from at ${getCurrentTimestampAsString()}")
            )
            _ <- runEtlJob(args, ejpm.job_deploy_mode, retry, spaced, fork)
          } yield ()
        else
          UIO(
            logger.info(
              s"Skipping inactive job ${db_job.job_name} submitted from $submitted_from at ${getCurrentTimestampAsString()}"
            )
          ) *> ZIO.fail(ExecutionError(s"Job ${db_job.job_name} is disabled"))
    } yield EtlJob(args.name, final_props)

  private def runEtlJob(
      args: EtlJobArgs,
      deploy_mode: Executor,
      retry: Int,
      spaced: Long,
      fork: Boolean
  ): RIO[ZEnv with JsonEnv with DBServerEnv, Unit] = {
    val actual_props = args.props.getOrElse(List.empty).map(x => (x.key, x.value)).toMap

    val jobRun: RIO[ZEnv with JsonEnv, Unit] =
      deploy_mode match {
        case lsp @ LOCAL_SUBPROCESS(_, _, _) =>
          LocalSubProcessExecutor(lsp).executeJob(args.name, actual_props)
        case LOCAL =>
          LocalExecutor[T]().executeJob(args.name, actual_props, config, java.util.UUID.randomUUID.toString)
        case dp @ DATAPROC(_, _, _, _, _) =>
          val main_class       = config.dataproc.map(_.mainclass).getOrElse("")
          val dp_libs          = config.dataproc.map(_.deplibs).getOrElse(List.empty)
          val actual_props_str = actual_props.map(x => s"${x._1}=${x._2}").mkString(",")
          val dp_args          = List("run_job", "--job_name", args.name, "--props", actual_props_str)
          DPJobApi
            .executeSparkJob(dp_args, main_class, dp_libs, dp.conf, dp.cluster, dp.project, dp.region)
            .provideLayer(DPJob.live(dp.endpoint))
        case LIVY(_) =>
          Task.fail(ExecutionError("Deploy mode LIVY not yet supported"))
        case KUBERNETES(_, _, _, _, _, _) =>
          Task.fail(ExecutionError("Deploy mode KUBERNETES not yet supported"))
      }

    val loggedJobRun: RIO[ZEnv with JsonEnv with DBServerEnv, Long] = jobRun
      .retry(Schedule.spaced(ZDuration.fromScala(Duration(spaced, MINUTES))) && Schedule.recurs(retry))
      .tapError(ex =>
        UIO(logger.error(ex.getMessage)) *> DBServerApi.updateFailedJob(args.name, getCurrentTimestamp)
      ) *> DBServerApi.updateSuccessJob(args.name, getCurrentTimestamp)

    blocking(if (fork) sem(args.name).withPermit(loggedJobRun).forkDaemon else sem(args.name).withPermit(loggedJobRun)).unit
  }
}
