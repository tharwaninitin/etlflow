package etlflow.webserver

import caliban.GraphQL.graphQL
import caliban.schema.GenericSchema
import caliban.{GraphQL, RootResolver}
import etlflow.api.APIEnv
import etlflow.api.Schema._
import etlflow.api.Service.login
import etlflow.jdbc.DBServerEnv
import zio.{Task, UIO, ZIO}

object GqlLoginAPI extends GenericSchema[APIEnv with DBServerEnv] {

  case class Mutations(login: UserArgs => ZIO[APIEnv with DBServerEnv, Throwable, UserAuth])
  case class Queries(dummy: Task[String])
  def dummyFunction: UIO[String] = Task.succeed("dummy")

  val api: GraphQL[APIEnv with DBServerEnv] =
    graphQL(
      RootResolver(
        Queries(dummyFunction),
        Mutations(
          args => login(args)
        )
      )
    )
}
