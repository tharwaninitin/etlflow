package etlflow.api

import etlflow.api.Schema.Creds.{AWS, JDBC}
import etlflow.api.Schema._
import etlflow.db._
import etlflow.executor.Executor
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.utils.DateTimeApi.{getCurrentTimestampAsString, getLocalDateTimeFromTimestamp, getTimestampAsString}
import etlflow.utils._
import etlflow.webserver.Authentication
import etlflow.{EJPMType, BuildInfo => BI}
import org.ocpsoft.prettytime.PrettyTime
import scalacache.caffeine.CaffeineCache
import zio.Fiber.Status.{Running, Suspended}
import zio.blocking.Blocking
import zio.{Task, UIO, ZIO, ZLayer, _}

import scala.reflect.runtime.universe.TypeTag

private[etlflow] object Implementation extends EtlFlowUtils with ApplicationLogger {

  def live[EJN <: EJPMType : TypeTag](auth: Authentication, enc: EncryptionAPI, executor: Executor[EJN], jobs: List[EtlJob], ejpm_package: String, supervisor: Supervisor[Chunk[Fiber.Runtime[Any, Any]]], cache: CaffeineCache[QueueDetails]): ZLayer[Blocking, Throwable, APIEnv] = {
    ZLayer.succeed(new Service {

      val pt = new PrettyTime()

      final private def monitorFibers(supervisor: Supervisor[Chunk[Fiber.Runtime[Any, Any]]]): UIO[List[EtlJobStatus]] = for {
        uio_status <- supervisor.value.map(_.map(_.dump))
        status     <- ZIO.collectAll(uio_status.toList)
        op = status.map{x =>
          val status = x.status match {
            case Running(_) => "Running"
            case Suspended(previous, interruptible, epoch, blockingOn, asyncTrace) => s"Suspended($asyncTrace)"
            case _ => "Other State"
          }
          EtlJobStatus(x.fiberId.seqNumber.toString, x.fiberName.getOrElse(""), getTimestampAsString(x.fiberId.startTimeMillis), "Scheduled", status)
        }
      } yield op

      override def getJobs: ZIO[ServerEnv, Throwable, List[Job]] = {
        for {
          jobs     <- DBApi.getJobs
          etljobs  <- ZIO.foreach(jobs)(x =>
            getJobPropsMapping[EJN](x.job_name,ejpm_package).map{props =>
              val lastRunTime = x.last_run_time.map(ts => pt.format(getLocalDateTimeFromTimestamp(ts))).getOrElse("")
              GetCronJob(x.schedule, x, lastRunTime, props)
            }
          )
        } yield etljobs
      }

      override def getCacheStats: ZIO[APIEnv with JsonEnv, Throwable, List[CacheDetails]] = {
        for {
          job_props <- CacheHelper.getCacheStats(jobPropsMappingCache, "JobProps")
          login     <- CacheHelper.getCacheStats(auth.cache, "Login")
        } yield (List(login,job_props))
      }

      override def getQueueStats: ZIO[APIEnv, Throwable, List[QueueDetails]] = UIO(CacheHelper.getValues(cache))

      override def getJobStats: ZIO[APIEnv, Throwable, List[EtlJobStatus]] = monitorFibers(supervisor)

      override def getJobLogs(args: JobLogsArgs): ZIO[APIEnv with DBEnv, Throwable, List[JobLogs]] = DBApi.getJobLogs(args)

      override def getCredentials: ZIO[APIEnv with DBEnv, Throwable, List[GetCredential]] = DBApi.getCredentials

      override def runJob(args: EtlJobArgs, submitter: String): RIO[ServerEnv, EtlJob] = executor.runActiveEtlJob(args, submitter)

      override def getDbStepRuns(args: DbStepRunArgs): ZIO[APIEnv with DBEnv, Throwable, List[StepRun]] = DBApi.getStepRuns(args)

      override def getDbJobRuns(args: DbJobRunArgs): ZIO[APIEnv with DBEnv, Throwable, List[JobRun]] = DBApi.getJobRuns(args)

      override def updateJobState(args: EtlJobStateArgs): ZIO[APIEnv with DBEnv, Throwable, Boolean] = DBApi.updateJobState(args)

      override def login(args: UserArgs): ZIO[APIEnv with DBEnv, Throwable, UserAuth] = auth.login(args)

      override def getInfo: ZIO[APIEnv, Throwable, EtlFlowMetrics] = Task {
        val dt = getLocalDateTimeFromTimestamp(BI.builtAtMillis)
        EtlFlowMetrics(
          0,
          0,
          jobs.length,
          jobs.length,
          build_time = s"${dt.toString.take(16)} ${pt.format(dt)}"
        )
      }

      override def getCurrentTime: ZIO[APIEnv, Throwable, CurrentTime] = UIO(CurrentTime(current_time = getCurrentTimestampAsString()))

      override def addCredentials(args: CredentialsArgs): RIO[ServerEnv, Credentials] = {
        for{
          value <- JsonApi.convertToString(args.value.map(x => (x.key, x.value)).toMap, List.empty)
          credentialDB = CredentialDB(
            args.name,
            args.`type` match {
              case JDBC => "jdbc"
              case AWS => "aws"
            },
            JsonString(value)
          )
          actualSerializerOutput <- enc.encryptCredential(credentialDB.`type`,credentialDB.value.str)
          addCredential <- DBApi.addCredential(credentialDB,JsonString(actualSerializerOutput))
        } yield addCredential
      }

      override def updateCredentials(args: CredentialsArgs): RIO[ServerEnv, Credentials] = {
        for{
          value <- JsonApi.convertToString(args.value.map(x => (x.key, x.value)).toMap, List.empty)
          credentialDB = CredentialDB(
            args.name,
            args.`type` match {
              case JDBC => "jdbc"
              case AWS => "aws"
            },
            JsonString(value)
          )
          actualSerializerOutput <- enc.encryptCredential(credentialDB.`type`,credentialDB.value.str)
          updateCredential <- DBApi.updateCredential(credentialDB,JsonString(actualSerializerOutput))
        } yield updateCredential
      }
    })
  }
}
