package etlflow.webserver

import caliban.GraphQL.graphQL
import caliban.schema.GenericSchema
import caliban.{GraphQL, RootResolver}
import etlflow.api.APIEnv
import etlflow.api.Schema._
import etlflow.api.Service.login
import etlflow.jdbc.{DBEnv}
import zio.{Task, UIO, ZIO}

object GqlLoginAPI extends GenericSchema[APIEnv with DBEnv] {

  case class Mutations(login: UserArgs => ZIO[APIEnv with DBEnv, Throwable, UserAuth])
  case class Queries(dummy: Task[String])
  def dummyFunction: UIO[String] = Task.succeed("dummy")

  val api: GraphQL[APIEnv with DBEnv] =
    graphQL(
      RootResolver(
        Queries(dummyFunction),
        Mutations(
          args => login(args)
        )
      )
    )
}
