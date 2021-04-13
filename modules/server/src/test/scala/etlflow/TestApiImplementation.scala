package etlflow

import doobie.hikari.HikariTransactor
import etlflow.utils.EtlFlowHelper.GQLEnv
import etlflow.webserver.api.ApiImplementation
import scalacache.caffeine.CaffeineCache
import zio.blocking.Blocking
import zio.{Task, ZLayer}

trait TestApiImplementation extends ServerSuiteHelper {
  def testHttp4s(transactor: HikariTransactor[Task], cache: CaffeineCache[String]): ZLayer[Blocking, Throwable, GQLEnv] =
    ApiImplementation.liveHttp4s[MEJP](transactor,cache,testCronJobs,Map.empty,List.empty,testJobsQueue,config)
}
