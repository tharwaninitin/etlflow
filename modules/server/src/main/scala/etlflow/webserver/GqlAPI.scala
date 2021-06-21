package etlflow.webserver

import caliban.CalibanError.ExecutionError
import caliban.GraphQL.graphQL
import caliban.Value.StringValue
import caliban.schema.{ArgBuilder, GenericSchema, Schema}
import caliban.{GraphQL, RootResolver}
import cron4s.{Cron, CronExpr}
import etlflow.api.APIEnv
import etlflow.api.Schema._
import etlflow.api.Service._
import etlflow.jdbc._
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock

import java.time.LocalDate
import java.time.format.DateTimeFormatter

private[etlflow] object GqlAPI extends GenericSchema[APIEnv with DBEnv with TransactorEnv with Blocking with Clock] {

  implicit val cronExprStringSchema: Schema[Any, CronExpr] = Schema.stringSchema.contramap(_.toString)
  implicit val cronExprArgBuilder: ArgBuilder[CronExpr] = {
    case StringValue(value) =>
      Cron(value).fold(ex => Left(ExecutionError(s"Can't parse $value into a Cron, error ${ex.getMessage}", innerThrowable = Some(ex))), Right(_))
    case other => Left(ExecutionError(s"Can't build a Cron from input $other"))
  }


  case class Queries(
                      jobs: ZIO[APIEnv with DBEnv, Throwable, List[Job]],
                      jobruns: DbJobRunArgs => ZIO[APIEnv with DBEnv, Throwable, List[JobRun]],
                      stepruns: DbStepRunArgs => ZIO[APIEnv with DBEnv, Throwable, List[StepRun]],
                      metrics: ZIO[APIEnv, Throwable, EtlFlowMetrics],
                      currentime: ZIO[APIEnv, Throwable, CurrentTime],
                      cacheStats:ZIO[APIEnv, Throwable, List[CacheDetails]],
                      queueStats:ZIO[APIEnv, Throwable, List[QueueDetails]],
                      jobLogs: JobLogsArgs => ZIO[APIEnv with DBEnv, Throwable, List[JobLogs]],
                      credential: ZIO[APIEnv with DBEnv, Throwable, List[GetCredential]],
                      jobStats: ZIO[APIEnv, Throwable, List[EtlJobStatus]]
  )

  case class Mutations(
                        run_job: EtlJobArgs => ZIO[APIEnv with DBEnv with TransactorEnv with Blocking with Clock, Throwable, EtlJob],
                        update_job_state: EtlJobStateArgs => ZIO[APIEnv with DBEnv, Throwable, Boolean],
                        add_credentials: CredentialsArgs => ZIO[APIEnv with DBEnv, Throwable, Credentials],
                        update_credentials: CredentialsArgs => ZIO[APIEnv with DBEnv, Throwable, Credentials],
                      )

  implicit val localDateExprStringSchema: Schema[Any, java.time.LocalDate] = Schema.stringSchema.contramap(_.toString)

  implicit val localDateExprArgBuilder: ArgBuilder[java.time.LocalDate] = {
    case StringValue(value) => Right(LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd")))
    case other => Left(ExecutionError(s"Can't build a date from input $other"))
  }

  val api: GraphQL[APIEnv with DBEnv with TransactorEnv with Clock with Blocking] =
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
          getCredentials,
          getJobStats
        ),
        Mutations(
          args => runJob(args,"GraphQL API").mapError(ex => ExecutionError(ex.getMessage)),
          args => updateJobState(args),
          args => addCredentials(args).mapError(ex => ExecutionError(ex.getMessage)),
          args => updateCredentials(args)
        )
      )
    )
}