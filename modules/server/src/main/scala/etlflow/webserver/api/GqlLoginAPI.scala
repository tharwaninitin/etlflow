package etlflow.webserver.api

import caliban.GraphQL.graphQL
import caliban.schema.GenericSchema
import caliban.{GraphQL, RootResolver}
import etlflow.utils.EtlFlowHelper._
import zio.{Task, UIO, ZIO}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import GqlService._

object GqlLoginAPI extends GenericSchema[GQLEnv] {

  case class Mutations(login: UserArgs => ZIO[GQLEnv, Throwable, UserAuth])
  case class Queries(dummy: Task[String])
  def dummyFunction: UIO[String] = Task.succeed("dummy")

  val api: GraphQL[Clock with Blocking with GQLEnv] =
    graphQL(
      RootResolver(
        Queries(dummyFunction),
        Mutations(
          args => login(args)
        )
      )
    )
}
