package etlflow.webserver

import caliban.GraphQL.graphQL
import caliban.schema.GenericSchema
import caliban.{GraphQL, RootResolver}
import etlflow.api.APIEnv
import etlflow.api.Schema._
import etlflow.api.Service.login
import etlflow.cache.CacheEnv
import etlflow.db.DBServerEnv
import zio.{Task, UIO, ZIO}

private[etlflow] object GqlLoginAPI extends GenericSchema[APIEnv with DBServerEnv with CacheEnv] {

  case class Mutations(login: UserArgs => ZIO[APIEnv with DBServerEnv with CacheEnv, Throwable, UserAuth])
  case class Queries(dummy: Task[String])
  def dummyFunction: UIO[String] = Task.succeed("dummy")

  val api: GraphQL[APIEnv with DBServerEnv with CacheEnv] =
    graphQL(
      RootResolver(
        Queries(dummyFunction),
        Mutations(
          args => login(args)
        )
      )
    )
}
