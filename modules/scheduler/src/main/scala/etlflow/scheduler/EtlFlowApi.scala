package etlflow.scheduler

import caliban.GraphQL.graphQL
import caliban.schema.GenericSchema
import caliban.{GraphQL, RootResolver}
import etlflow.scheduler.EtlFlowHelper._
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.stream.ZStream

object EtlFlowApi extends GenericSchema[EtlFlowHas] {

  case class Queries(
                      etljobs: ZIO[EtlFlowHas, Throwable, List[EtlJob]],
                      metrics: ZIO[EtlFlowHas, Throwable, EtlFlowMetrics],
                    )

  case class Mutations(
                        run_job: EtlJobArgs => ZIO[EtlFlowHas, Throwable, EtlJob],
                        login: UserArgs => ZIO[EtlFlowHas, Throwable, UserAuth]
                      )

  case class Subscriptions(notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus])

  val api: GraphQL[Console with Clock with Blocking with EtlFlowHas] =
    graphQL(
      RootResolver(
        Queries(
          getEtlJobs,
          getInfo,
        ),
        Mutations(
          args => runJob(args),
          args => login(args)
        ),
        Subscriptions(notifications)
      )
    )
}
