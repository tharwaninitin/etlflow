package etlflow.webserver

import caliban.CalibanError.ExecutionError
import caliban.GraphQL.graphQL
import caliban.Value.StringValue
import caliban.schema.{ArgBuilder, GenericSchema, Schema}
import caliban.{GraphQL, RootResolver}
import cron4s.{Cron, CronExpr}
import etlflow.DBEnv
import etlflow.api.APIEnv
import etlflow.api.Schema._
import etlflow.api.Service._
import etlflow.jdbc.DBServerEnv
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.stream.ZStream

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object GqlAPI extends GenericSchema[APIEnv with DBServerEnv with DBEnv with Blocking with Clock] {

  implicit val cronExprStringSchema: Schema[Any, CronExpr] = Schema.stringSchema.contramap(_.toString)
  implicit val cronExprArgBuilder: ArgBuilder[CronExpr] = {
    case StringValue(value) =>
      Cron(value).fold(ex => Left(ExecutionError(s"Can't parse $value into a Cron, error ${ex.getMessage}", innerThrowable = Some(ex))), Right(_))
    case other => Left(ExecutionError(s"Can't build a Cron from input $other"))
  }


  case class Queries(
                      jobs: ZIO[APIEnv with DBServerEnv, Throwable, List[Job]],
                      jobruns: DbJobRunArgs => ZIO[APIEnv with DBServerEnv, Throwable, List[JobRun]],
                      stepruns: DbStepRunArgs => ZIO[APIEnv with DBServerEnv, Throwable, List[StepRun]],
                      metrics: ZIO[APIEnv, Throwable, EtlFlowMetrics],
                      currentime: ZIO[APIEnv, Throwable, CurrentTime],
                      cacheStats:ZIO[APIEnv, Throwable, List[CacheDetails]],
                      queueStats:ZIO[APIEnv, Throwable, List[QueueDetails]],
                      jobLogs: JobLogsArgs => ZIO[APIEnv with DBServerEnv, Throwable, List[JobLogs]],
                      credential: ZIO[APIEnv with DBServerEnv, Throwable, List[GetCredential]]

  )

  case class Mutations(
                        run_job: EtlJobArgs => ZIO[APIEnv with DBServerEnv with DBEnv with Blocking with Clock, Throwable, EtlJob],
                        update_job_state: EtlJobStateArgs => ZIO[APIEnv with DBServerEnv, Throwable, Boolean],
                        add_credentials: CredentialsArgs => ZIO[APIEnv with DBServerEnv, Throwable, Credentials],
                        update_credentials: CredentialsArgs => ZIO[APIEnv with DBServerEnv, Throwable, Credentials],
                      )

  case class Subscriptions(notifications: ZStream[APIEnv, Nothing, EtlJobStatus])

  implicit val localDateExprStringSchema: Schema[Any, java.time.LocalDate] = Schema.stringSchema.contramap(_.toString)

  implicit val localDateExprArgBuilder: ArgBuilder[java.time.LocalDate] = {
    case StringValue(value) => Right(LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd")))
    case other => Left(ExecutionError(s"Can't build a date from input $other"))
  }

  val api: GraphQL[APIEnv with DBServerEnv with DBEnv with Clock with Blocking] =
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