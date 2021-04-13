package etlflow

import doobie.hikari.HikariTransactor
import etlflow.utils.EtlFlowHelper.GQLEnv
import etlflow.webserver.api.ApiImplementation
import zio.blocking.Blocking
import zio.{Task, ZLayer}

trait TestApiImplementation extends ServerSuiteHelper {
  def testHttp4s(transactor: HikariTransactor[Task]): ZLayer[Blocking, Throwable, GQLEnv] =
    ApiImplementation.live[MEJP](transactor,cache,testCronJobs,testJobsSemaphore,List.empty,testJobsQueue,config)
}
