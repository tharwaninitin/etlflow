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
                      etljobs: EtlJobNameArgs => ZIO[EtlFlowHas, Throwable, List[EtlJob]],
                      info: ZIO[EtlFlowHas, Throwable, EtlFlowInfo],
                      // get_stream: ZStream[EtlFlowHas, Throwable, EtlFlowInfo],
                      // get_logs: ZIO[EtlFlowHas, Throwable, EtlFlowInfo]
                    )

  case class Mutations(run_job: EtlJobArgs => ZIO[EtlFlowHas, Throwable, EtlJob])

  case class Subscriptions(notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus])

  val api: GraphQL[Console with Clock with Blocking with EtlFlowHas] =
    graphQL(
      RootResolver(
        Queries(
          args => getEtlJobs(args),
          getInfo,
          //getStream,
          //getLogs
        ),
        Mutations(args => runJob(args)),
        Subscriptions(notifications)
      )
    )
}
