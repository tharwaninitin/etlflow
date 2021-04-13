package etlflow.webserver.api

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import caliban.CalibanError.ExecutionError
import caliban.GraphQL.graphQL
import caliban.Value.StringValue
import caliban.schema.{ArgBuilder, GenericSchema, Schema}
import caliban.{GraphQL, RootResolver}
import cron4s.{Cron, CronExpr}
import etlflow.log.{JobRun, StepRun}
import etlflow.utils.EtlFlowHelper.{JobLogsArgs, _}
import ApiService._
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.stream.ZStream

object GqlAPI extends GenericSchema[GQLEnv with Blocking with Clock] {

  implicit val cronExprStringSchema: Schema[Any, CronExpr] = Schema.stringSchema.contramap(_.toString)
  implicit val cronExprArgBuilder: ArgBuilder[CronExpr] = {
    case StringValue(value) =>
      Cron(value).fold(ex => Left(ExecutionError(s"Can't parse $value into a Cron, error ${ex.getMessage}", innerThrowable = Some(ex))), Right(_))
    case other => Left(ExecutionError(s"Can't build a Cron from input $other"))
  }


  case class Queries(
                      jobs: ZIO[GQLEnv, Throwable, List[Job]],
                      jobruns: DbJobRunArgs => ZIO[GQLEnv, Throwable, List[JobRun]],
                      stepruns: DbStepRunArgs => ZIO[GQLEnv, Throwable, List[StepRun]],
                      metrics: ZIO[GQLEnv, Throwable, EtlFlowMetrics],
                      currentime: ZIO[GQLEnv, Throwable, CurrentTime],
                      cacheStats:ZIO[GQLEnv, Throwable, List[CacheDetails]],
                      queueStats:ZIO[GQLEnv, Throwable, List[QueueDetails]],
                      jobLogs: JobLogsArgs => ZIO[GQLEnv, Throwable, List[JobLogs]],
                      credential: ZIO[GQLEnv, Throwable, List[UpdateCredentialDB]]

  )

  case class Mutations(
                        run_job: EtlJobArgs => ZIO[GQLEnv with Blocking with Clock, Throwable, EtlJob],
                        update_job_state: EtlJobStateArgs => ZIO[GQLEnv, Throwable, Boolean],
                        add_cron_job: CronJobArgs => ZIO[GQLEnv, Throwable, CronJob],
                        update_cron_job: CronJobArgs => ZIO[GQLEnv, Throwable, CronJob],
                        add_credentials: CredentialsArgs => ZIO[GQLEnv, Throwable, Credentials],
                        update_credentials: CredentialsArgs => ZIO[GQLEnv, Throwable, Credentials],
                      )

  case class Subscriptions(notifications: ZStream[GQLEnv, Nothing, EtlJobStatus])

  implicit val localDateExprStringSchema: Schema[Any, java.time.LocalDate] = Schema.stringSchema.contramap(_.toString)

  implicit val localDateExprArgBuilder: ArgBuilder[java.time.LocalDate] = {
    case StringValue(value) => Right(LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd")))
    case other => Left(ExecutionError(s"Can't build a date from input $other"))
  }

  val api: GraphQL[Clock with Blocking with GQLEnv] =
    graphQL(
      RootResolver(
        Queries(
          getJobs,
          args => getDbJobRuns(args),
          args => getDbStepRuns(args),
          getInfo,
          getCurrentTime,
          getCacheStats,
          getQueueStats,
          args => getJobLogs(args),
          getCredentials
        ),
        Mutations(
          args => runJob(args,"GraphQL API").mapError(ex => ExecutionError(ex.getMessage)),
          args => updateJobState(args),
          args => addCronJob(args),
          args => updateCronJob(args),
          args => addCredentials(args),
          args => updateCredentials(args)
        ),
        Subscriptions(
          notifications
        )
      )
    )
}