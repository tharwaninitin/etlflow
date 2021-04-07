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
import GqlService._
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.stream.ZStream

object GqlAPI extends GenericSchema[EtlFlowHas] {

  implicit val cronExprStringSchema: Schema[Any, CronExpr] = Schema.stringSchema.contramap(_.toString)
  implicit val cronExprArgBuilder: ArgBuilder[CronExpr] = {
    case StringValue(value) =>
      Cron(value).fold(ex => Left(ExecutionError(s"Can't parse $value into a Cron, error ${ex.getMessage}", innerThrowable = Some(ex))), Right(_))
    case other => Left(ExecutionError(s"Can't build a Cron from input $other"))
  }


  case class Queries(
                      jobs: ZIO[EtlFlowHas, Throwable, List[Job]],
                      jobruns: DbJobRunArgs => ZIO[EtlFlowHas, Throwable, List[JobRun]],
                      stepruns: DbStepRunArgs => ZIO[EtlFlowHas, Throwable, List[StepRun]],
                      metrics: ZIO[EtlFlowHas, Throwable, EtlFlowMetrics],
                      currentime: ZIO[EtlFlowHas, Throwable, CurrentTime],
                      cacheStats:ZIO[EtlFlowHas, Throwable, List[CacheDetails]],
                      queueStats:ZIO[EtlFlowHas, Throwable, List[QueueDetails]],
                      jobLogs: JobLogsArgs => ZIO[EtlFlowHas, Throwable, List[JobLogs]]

  )

  case class Mutations(
                        run_job: EtlJobArgs => ZIO[EtlFlowHas, Throwable, EtlJob],
                        update_job_state: EtlJobStateArgs => ZIO[EtlFlowHas, Throwable, Boolean],
                        add_cron_job: CronJobArgs => ZIO[EtlFlowHas, Throwable, CronJob],
                        update_cron_job: CronJobArgs => ZIO[EtlFlowHas, Throwable, CronJob],
                        add_credentials: CredentialsArgs => ZIO[EtlFlowHas, Throwable, Credentials],
                        update_credentials: CredentialsArgs => ZIO[EtlFlowHas, Throwable, Credentials],
                      )

  case class Subscriptions(notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus])

  implicit val localDateExprStringSchema: Schema[Any, java.time.LocalDate] = Schema.stringSchema.contramap(_.toString)

  implicit val localDateExprArgBuilder: ArgBuilder[java.time.LocalDate] = {
    case StringValue(value) => Right(LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd")))
    case other => Left(ExecutionError(s"Can't build a date from input $other"))
  }

  val api: GraphQL[Console with Clock with Blocking with EtlFlowHas] =
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
          args => getJobLogs(args)
        ),
        Mutations(
          args => runJob(args),
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