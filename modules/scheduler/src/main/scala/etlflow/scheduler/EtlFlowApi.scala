package etlflow.scheduler

import caliban.CalibanError.ExecutionError
import caliban.GraphQL.graphQL
import caliban.Value.StringValue
import caliban.schema._
import caliban.{GraphQL, RootResolver}
import cron4s._
import etlflow.scheduler.EtlFlowHelper._
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
                      etljobs: ZIO[EtlFlowHas, Throwable, List[EtlJob]],
                      cronjobs: ZIO[EtlFlowHas, Throwable, List[CronJob]],
                      metrics: ZIO[EtlFlowHas, Throwable, EtlFlowMetrics],
                    )

  case class Mutations(
                        run_job: EtlJobArgs => ZIO[EtlFlowHas, Throwable, EtlJob],
                        add_cron_job: CronJob => ZIO[EtlFlowHas, Throwable, CronJob],
                        login: UserArgs => ZIO[EtlFlowHas, Throwable, UserAuth]
                      )

  case class Subscriptions(notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus])

  val api: GraphQL[Console with Clock with Blocking with EtlFlowHas] =
    graphQL(
      RootResolver(
        Queries(
          getEtlJobs,
          getCronJobs,
          getInfo,
        ),
        Mutations(
          args => runJob(args),
          args => addCronJob(args),
          args => login(args)
        ),
        Subscriptions(notifications)
      )
    )
}
