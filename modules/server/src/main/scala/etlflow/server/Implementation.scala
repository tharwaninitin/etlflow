package etlflow.server

import crypto4s.Crypto
import etlflow.db._
import etlflow.server.model._
import etlflow.executor.ServerExecutor
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.model.Credential
import etlflow.utils.DateTimeApi.{getCurrentTimestampAsString, getLocalDateTimeFromTimestamp}
import etlflow.utils.{ReflectAPI => RF, _}
import etlflow.webserver.Authentication
import etlflow.{BuildInfo => BI, EJPMType}
import etlflow.json.CredentialImplicits._
import org.ocpsoft.prettytime.PrettyTime
import zio._
import zio.blocking.Blocking

private[etlflow] object Implementation extends ApplicationLogger {

  def live[T <: EJPMType: Tag](
      auth: Authentication,
      executor: ServerExecutor[T],
      jobs: List[EtlJob],
      crypto: Crypto
  ): ZLayer[Blocking, Throwable, APIEnv] =
    ZLayer.succeed(new Service {

      val pt = new PrettyTime()

      override def getJobs: RIO[DBServerEnv, List[Job]] = for {
        jobs <- DBServerApi.getJobs
        etljobs <- ZIO.foreach(jobs)(x =>
          RF.getJob[T](x.job_name).map { ejpm =>
            val lastRunTime = x.last_run_time.map(ts => pt.format(getLocalDateTimeFromTimestamp(ts))).getOrElse("")
            GetJob(x.schedule, x, lastRunTime, ejpm.getProps)
          }
        )
      } yield etljobs

      override def getJobLogs(args: JobLogsArgs): RIO[DBServerEnv, List[JobLogs]] = DBServerApi.getJobLogs(args)

      override def getCredentials: RIO[DBServerEnv, List[GetCredential]] = DBServerApi.getCredentials

      override def runJob(
          name: String,
          props: Map[String, String],
          submitter: String
      ): RIO[ZEnv with JsonEnv with DBServerEnv, EtlJob] =
        executor.runActiveEtlJob(name, props, submitter)

      override def getStepRuns(args: DbStepRunArgs): RIO[DBServerEnv, List[StepRun]] = DBServerApi.getStepRuns(args)

      override def getJobRuns(args: DbJobRunArgs): RIO[DBServerEnv, List[JobRun]] = DBServerApi.getJobRuns(args)

      override def updateJobState(args: EtlJobStateArgs): RIO[DBServerEnv, Boolean] = DBServerApi.updateJobState(args)

      override def login(args: UserArgs): RIO[DBServerEnv, UserAuth] = auth.login(args)

      override def getMetrics: UIO[EtlFlowMetrics] = UIO {
        val dt = getLocalDateTimeFromTimestamp(BI.builtAtMillis)
        EtlFlowMetrics(
          0,
          0,
          jobs.length,
          jobs.length,
          build_time = s"${dt.toString.take(16)} ${pt.format(dt)}"
        )
      }

      override def getCurrentTime: UIO[String] = UIO(getCurrentTimestampAsString())

      def encryptCredential(`type`: String, value: String): RIO[JsonEnv, String] =
        `type` match {
          case "jdbc" =>
            for {
              jdbc <- JsonApi.convertToObject[Credential.JDBC](value)
              encrypt_user     = crypto.encrypt(jdbc.user)
              encrypt_password = crypto.encrypt(jdbc.password)
              json <- JsonApi.convertToString(Credential.JDBC(jdbc.url, encrypt_user, encrypt_password, jdbc.driver))
            } yield json
          case "aws" =>
            for {
              aws <- JsonApi.convertToObject[Credential.AWS](value)
              encrypt_access_key = crypto.encrypt(aws.access_key)
              encrypt_secret_key = crypto.encrypt(aws.secret_key)
              json <- JsonApi.convertToString(Credential.AWS(encrypt_access_key, encrypt_secret_key))
            } yield json
        }

      override def addCredential(args: CredentialsArgs): RIO[JsonEnv with DBServerEnv, etlflow.server.model.Credential] =
        for {
          json <- JsonApi.convertToString(args.value.map(x => (x.key, x.value)).toMap)
          cred_type = args.`type` match {
            case Creds.JDBC => "jdbc"
            case Creds.AWS  => "aws"
          }
          enc_json <- encryptCredential(cred_type, json)
          cred = etlflow.server.model.Credential(args.name, cred_type, enc_json)
          addCredential <- DBServerApi.addCredential(cred)
        } yield addCredential

      override def updateCredential(args: CredentialsArgs): RIO[JsonEnv with DBServerEnv, etlflow.server.model.Credential] =
        for {
          json <- JsonApi.convertToString(args.value.map(x => (x.key, x.value)).toMap)
          cred_type = args.`type` match {
            case Creds.JDBC => "jdbc"
            case Creds.AWS  => "aws"
          }
          enc_json <- encryptCredential(cred_type, json)
          cred = etlflow.server.model.Credential(args.name, cred_type, enc_json)
          updateCredential <- DBServerApi.updateCredential(cred)
        } yield updateCredential
    })
}
