package etlflow.webserver.api

import caliban.GraphQL.graphQL
import caliban.schema.GenericSchema
import caliban.{GraphQL, RootResolver}
import etlflow.utils.EtlFlowHelper._
import zio.{Task, UIO, ZIO}
import ApiService.login
import etlflow.jdbc.DBEnv

object GqlLoginAPI extends GenericSchema[GQLEnv with DBEnv] {

  case class Mutations(login: UserArgs => ZIO[GQLEnv with DBEnv, Throwable, UserAuth])
  case class Queries(dummy: Task[String])
  def dummyFunction: UIO[String] = Task.succeed("dummy")

  val api: GraphQL[GQLEnv with DBEnv] =
    graphQL(
      RootResolver(
        Queries(dummyFunction),
        Mutations(
          args => login(args)
        )
      )
    )
}
