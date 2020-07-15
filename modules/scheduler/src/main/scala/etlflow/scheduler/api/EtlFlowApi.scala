package etlflow.scheduler.api

import caliban.CalibanError.ExecutionError
import caliban.GraphQL.graphQL
import caliban.Value.StringValue
import caliban.schema.{ArgBuilder, GenericSchema, Schema}
import caliban.{GraphQL, RootResolver}
import cron4s.{Cron, CronExpr}
import etlflow.log.{JobRun, StepRun}
import etlflow.scheduler.api.EtlFlowHelper._
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.stream.ZStream

object EtlFlowApi extends GenericSchema[EtlFlowHas] {

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
                    )

  case class Mutations(
                        run_job: EtlJobArgs => ZIO[EtlFlowHas, Throwable, EtlJob],
                        update_job_state: EtlJobStateArgs => ZIO[EtlFlowHas, Throwable, Boolean],
                        add_cron_job: CronJobArgs => ZIO[EtlFlowHas, Throwable, CronJob],
                        update_cron_job: CronJobArgs => ZIO[EtlFlowHas, Throwable, CronJob],
                      )

  case class Subscriptions(
                            notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus],
                            getStream: ZStream[EtlFlowHas, Nothing, EtlFlowMetrics]
                          )

  val api: GraphQL[Console with Clock with Blocking with EtlFlowHas] =
    graphQL(
      RootResolver(
        Queries(
          getJobs,
          args => getDbJobRuns(args),
          args => getDbStepRuns(args),
          getInfo,
        ),
        Mutations(
          args => runJob(args),
          args => updateJobState(args),
          args => addCronJob(args),
          args => updateCronJob(args),
        ),
        Subscriptions(
          notifications,
          getStream
        )
      )
    )
}
