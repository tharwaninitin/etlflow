package etlflow.webserver

import caliban.CalibanError.ExecutionError
import caliban.GraphQL.graphQL
import caliban.schema.{GenericSchema, Schema}
import caliban.{GraphQL, RootResolver}
import cron4s.CronExpr
import etlflow.api.Schema._
import etlflow.api.Service._
import etlflow.api.{APIEnv, ServerEnv}
import etlflow.db._
import etlflow.json.JsonEnv
import zio.ZIO

private[etlflow] object GqlAPI extends GenericSchema[ServerEnv] {

  implicit val cronExprStringSchema: Schema[Any, CronExpr] = Schema.stringSchema.contramap(_.toString)
//  implicit val cronExprArgBuilder: ArgBuilder[CronExpr] = {
//    case StringValue(value) =>
//      Cron(value).fold(ex => Left(ExecutionError(s"Can't parse $value into a Cron, error ${ex.getMessage}", innerThrowable = Some(ex))), Right(_))
//    case other => Left(ExecutionError(s"Can't build a Cron from input $other"))
//  }

  case class Queries(
                      jobs: ZIO[ServerEnv, Throwable, List[Job]],
                      jobruns: DbJobRunArgs => ZIO[APIEnv with DBEnv, Throwable, List[JobRun]],
                      stepruns: DbStepRunArgs => ZIO[APIEnv with DBEnv, Throwable, List[StepRun]],
                      metrics: ZIO[APIEnv, Throwable, EtlFlowMetrics],
                      currentime: ZIO[APIEnv, Throwable, CurrentTime],
                      cacheStats:ZIO[APIEnv with JsonEnv, Throwable, List[CacheDetails]],
                      queueStats:ZIO[APIEnv, Throwable, List[QueueDetails]],
                      jobLogs: JobLogsArgs => ZIO[APIEnv with DBEnv, Throwable, List[JobLogs]],
                      credential: ZIO[APIEnv with DBEnv, Throwable, List[GetCredential]],
                      jobStats: ZIO[APIEnv, Throwable, List[EtlJobStatus]]
  )

  case class Mutations(
                        run_job: EtlJobArgs => ZIO[ServerEnv, Throwable, EtlJob],
                        update_job_state: EtlJobStateArgs => ZIO[APIEnv with DBEnv, Throwable, Boolean],
                        add_credentials: CredentialsArgs => ZIO[ServerEnv, Throwable, Credentials],
                        update_credentials: CredentialsArgs => ZIO[ServerEnv, Throwable, Credentials],
                      )

//  implicit val localDateExprStringSchema: Schema[Any, java.time.LocalDate] = Schema.stringSchema.contramap(_.toString)
//
//  implicit val localDateExprArgBuilder: ArgBuilder[java.time.LocalDate] = {
//    case StringValue(value) => Right(LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd")))
//    case other => Left(ExecutionError(s"Can't build a date from input $other"))
//  }

  val api: GraphQL[ServerEnv] =
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