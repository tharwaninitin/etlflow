package etlflow.api

import etlflow.api.Schema.Creds.{AWS, JDBC}
import etlflow.api.Schema._
import etlflow.cache._
import etlflow.crypto.{CryptoApi, CryptoEnv}
import etlflow.db._
import etlflow.executor.Executor
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.schema.Credential
import etlflow.utils.DateTimeApi.{getCurrentTimestampAsString, getLocalDateTimeFromTimestamp, getTimestampAsString}
import etlflow.utils.{ReflectAPI => RF, _}
import etlflow.webserver.Authentication
import etlflow.{EJPMType, BuildInfo => BI}
import org.ocpsoft.prettytime.PrettyTime
import zio.Fiber.Status.{Running, Suspended}
import zio.blocking.Blocking
import zio._
import io.circe.generic.auto._

private[etlflow] object Implementation extends ApplicationLogger {

  def live[T <: EJPMType : Tag](auth: Authentication, executor: Executor[T], jobs: List[EtlJob], supervisor: Supervisor[Chunk[Fiber.Runtime[Any, Any]]], cache: Cache[QueueDetails]): ZLayer[Blocking, Throwable, APIEnv] = {
    ZLayer.succeed(new Service {

      val pt = new PrettyTime()

      final private def monitorFibers(supervisor: Supervisor[Chunk[Fiber.Runtime[Any, Any]]]): UIO[List[EtlJobStatus]] = for {
        uio_status <- supervisor.value.map(_.map(_.dump))
        status     <- ZIO.collectAll(uio_status.toList)
        op = status.map{x =>
          val status = x.status match {
            case Running(_) => "Running"
            case Suspended(_, _, _, _, asyncTrace) => s"Suspended($asyncTrace)"
            case _ => "Other State"
          }
          EtlJobStatus(x.fiberId.seqNumber.toString, x.fiberName.getOrElse(""), getTimestampAsString(x.fiberId.startTimeMillis), "Scheduled", status)
        }
      } yield op

      override def getJobs: ZIO[ServerEnv, Throwable, List[Job]] = {
        for {
          jobs     <- DBServerApi.getJobs
          etljobs  <- ZIO.foreach(jobs)(x =>
            RF.getJob[T](x.job_name).map{ejpm =>
              val lastRunTime = x.last_run_time.map(ts => pt.format(getLocalDateTimeFromTimestamp(ts))).getOrElse("")
              GetCronJob(x.schedule, x, lastRunTime, ejpm.getProps)
            }
          )
        } yield etljobs
      }

      override def getCacheStats: ZIO[APIEnv with CacheEnv, Throwable, List[CacheDetails]] = {
        for {
          //job_props <- CacheApi.getCacheStats(jobPropsMappingCache, "JobProps")
          login     <- CacheApi.getStats(auth.cache, "Login")
        } yield (List(login))
      }

      override def getQueueStats: ZIO[APIEnv with CacheEnv, Throwable, List[QueueDetails]] = {
        for {
          job_props <- CacheApi.getValues(cache)
        } yield (job_props)
      }

      override def getJobStats: ZIO[APIEnv, Throwable, List[EtlJobStatus]] = monitorFibers(supervisor)

      override def getJobLogs(args: JobLogsArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobLogs]] = DBServerApi.getJobLogs(args)

      override def getCredentials: ZIO[APIEnv with DBServerEnv, Throwable, List[GetCredential]] = DBServerApi.getCredentials

      override def runJob(args: EtlJobArgs, submitter: String): RIO[ServerEnv, EtlJob] = executor.runActiveEtlJob(args, submitter)

      override def getDbStepRuns(args: DbStepRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[StepRun]] = DBServerApi.getStepRuns(args)

      override def getDbJobRuns(args: DbJobRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobRun]] = DBServerApi.getJobRuns(args)

      override def updateJobState(args: EtlJobStateArgs): ZIO[APIEnv with DBServerEnv, Throwable, Boolean] = DBServerApi.updateJobState(args)

      override def login(args: UserArgs): ZIO[APIEnv with DBServerEnv with CacheEnv, Throwable, UserAuth] = auth.login(args)

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

      def encryptCredential(`type`: String, value: String): RIO[CryptoEnv with JsonEnv,String] = {
        `type` match {
          case "jdbc" =>
            for {
              jdbc                <- JsonApi.convertToObject[Credential.JDBC](value)
              encrypt_user        <- CryptoApi.encrypt(jdbc.user)
              encrypt_password    <- CryptoApi.encrypt(jdbc.password)
              json <- JsonApi.convertToString(Credential.JDBC(jdbc.url, encrypt_user, encrypt_password, jdbc.driver), List.empty)
            } yield json
          case "aws" =>
            for {
              aws  <- JsonApi.convertToObject[Credential.AWS](value)
              encrypt_access_key <- CryptoApi.encrypt(aws.access_key)
              encrypt_secret_key <- CryptoApi.encrypt(aws.secret_key)
              json <- JsonApi.convertToString(Credential.AWS(encrypt_access_key, encrypt_secret_key), List.empty)
            } yield json
        }
      }

      override def addCredentials(args: CredentialsArgs): RIO[ServerEnv, etlflow.db.Credential] = {
        for{
          json <- JsonApi.convertToString(args.value.map(x => (x.key, x.value)).toMap, List.empty)
          cred_type = args.`type` match {
                        case JDBC => "jdbc"
                        case AWS => "aws"
                      }
          enc_json <- encryptCredential(cred_type, json)
          cred = etlflow.db.Credential(args.name, cred_type, enc_json)
          addCredential <- DBServerApi.addCredential(cred)
        } yield addCredential
      }

      override def updateCredentials(args: CredentialsArgs): RIO[ServerEnv, etlflow.db.Credential] = {
        for{
          json <- JsonApi.convertToString(args.value.map(x => (x.key, x.value)).toMap, List.empty)
          cred_type = args.`type` match {
                        case JDBC => "jdbc"
                        case AWS => "aws"
                      }
          enc_json <- encryptCredential(cred_type,json)
          cred = etlflow.db.Credential(args.name, cred_type, enc_json)
          updateCredential <- DBServerApi.updateCredential(cred)
        } yield updateCredential
      }
    })
  }
}
