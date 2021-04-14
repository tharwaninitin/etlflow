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
import etlflow.jdbc.DBEnv
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.stream.ZStream

object GqlAPI extends GenericSchema[GQLEnv with DBEnv with Blocking with Clock] {

  implicit val cronExprStringSchema: Schema[Any, CronExpr] = Schema.stringSchema.contramap(_.toString)
  implicit val cronExprArgBuilder: ArgBuilder[CronExpr] = {
    case StringValue(value) =>
      Cron(value).fold(ex => Left(ExecutionError(s"Can't parse $value into a Cron, error ${ex.getMessage}", innerThrowable = Some(ex))), Right(_))
    case other => Left(ExecutionError(s"Can't build a Cron from input $other"))
  }


  case class Queries(
                      jobs: ZIO[GQLEnv with DBEnv, Throwable, List[Job]],
                      jobruns: DbJobRunArgs => ZIO[GQLEnv with DBEnv, Throwable, List[JobRun]],
                      stepruns: DbStepRunArgs => ZIO[GQLEnv with DBEnv, Throwable, List[StepRun]],
                      metrics: ZIO[GQLEnv, Throwable, EtlFlowMetrics],
                      currentime: ZIO[GQLEnv, Throwable, CurrentTime],
                      cacheStats:ZIO[GQLEnv, Throwable, List[CacheDetails]],
                      queueStats:ZIO[GQLEnv, Throwable, List[QueueDetails]],
                      jobLogs: JobLogsArgs => ZIO[GQLEnv with DBEnv, Throwable, List[JobLogs]],
                      credential: ZIO[GQLEnv with DBEnv, Throwable, List[UpdateCredentialDB]]

  )

  case class Mutations(
                        run_job: EtlJobArgs => ZIO[GQLEnv with DBEnv with Blocking with Clock, Throwable, EtlJob],
                        update_job_state: EtlJobStateArgs => ZIO[GQLEnv with DBEnv, Throwable, Boolean],
                        add_credentials: CredentialsArgs => ZIO[GQLEnv with DBEnv, Throwable, Credentials],
                        update_credentials: CredentialsArgs => ZIO[GQLEnv with DBEnv, Throwable, Credentials],
                      )

  case class Subscriptions(notifications: ZStream[GQLEnv, Nothing, EtlJobStatus])

  implicit val localDateExprStringSchema: Schema[Any, java.time.LocalDate] = Schema.stringSchema.contramap(_.toString)

  implicit val localDateExprArgBuilder: ArgBuilder[java.time.LocalDate] = {
    case StringValue(value) => Right(LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd")))
    case other => Left(ExecutionError(s"Can't build a date from input $other"))
  }

  val api: GraphQL[GQLEnv with DBEnv with Clock with Blocking] =
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
          args => addCredentials(args),
          args => updateCredentials(args)
        ),
        Subscriptions(
          notifications
        )
      )
    )
}