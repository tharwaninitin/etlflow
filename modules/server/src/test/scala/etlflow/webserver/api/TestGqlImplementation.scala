package etlflow.webserver.api

import doobie.hikari.HikariTransactor
import etlflow.ServerSuiteHelper
import etlflow.utils.EtlFlowHelper._
import scalacache.caffeine.CaffeineCache
import zio._
import zio.blocking.Blocking

trait TestGqlImplementation extends ServerSuiteHelper with GqlImplementation {

  def testHttp4s(transactor: HikariTransactor[Task], cache: CaffeineCache[String]): ZLayer[Blocking, Throwable, GQLEnv] =
    liveHttp4s[MEJP](transactor,cache,testCronJobs,Map.empty,List.empty,testJobsQueue)
}
