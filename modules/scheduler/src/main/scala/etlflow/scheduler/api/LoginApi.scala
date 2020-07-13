package etlflow.scheduler.api

import caliban.GraphQL.graphQL
import caliban.schema.GenericSchema
import caliban.{GraphQL, RootResolver}
import etlflow.scheduler.api.EtlFlowHelper._
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console

object LoginApi extends GenericSchema[EtlFlowHas] {

  case class Mutations(login: UserArgs => ZIO[EtlFlowHas, Throwable, UserAuth])

  val api: GraphQL[Console with Clock with Blocking with EtlFlowHas] =
    graphQL(
      RootResolver(
        None,
        Mutations(
          args => login(args)
        ),
        None
      )
    )
}
