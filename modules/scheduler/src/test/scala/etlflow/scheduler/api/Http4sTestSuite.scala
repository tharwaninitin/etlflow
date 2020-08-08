package etlflow.scheduler.api

import caliban.{CalibanError, GraphQLInterpreter, Http4sAdapter}
import etlflow.scheduler.api.EtlFlowHelper.EtlFlowHas
import etlflow.scheduler.{CacheHelper, TestSchedulerApp, TestSuiteHelper}
import io.circe.Json
import org.http4s.HttpRoutes
import org.http4s.client.Client
import org.http4s.server.Router
import org.http4s.implicits._
import org.http4s._
import org.http4s.circe._
import org.http4s.dsl.io._
import zio.{IO, RIO, Runtime, Task, ZEnv, ZIO, ZLayer}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM, suite, testM}
import zio.interop.catz._

object Http4sTestSuite extends DefaultRunnableSpec with TestSuiteHelper with TestSchedulerApp {

  val env: ZLayer[Any, Throwable, Clock with Blocking with EtlFlowHas with Console] =
    Clock.live ++ Blocking.live ++ testHttp4s(transactor,cache) ++ Console.live

  val etlFlowInterpreter: Task[GraphQLInterpreter[Console with Clock with Blocking with EtlFlowHas, CalibanError]] =
    EtlFlowApi.api.interpreter

  type EtlFlowTask[A] = RIO[ZEnv with EtlFlowHas, A]

  val runtime: Runtime[ZEnv] = Runtime.default

  val interpreter = runtime.unsafeRun(etlFlowInterpreter)

  val routes: HttpRoutes[EtlFlowTask] = Router[EtlFlowTask](
    "/api/etlflow" -> Http4sAdapter.makeHttpService(interpreter)
  )

  val client: Client[EtlFlowTask] = Client.fromHttpApp[EtlFlowTask](routes.orNotFound)


  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("Scheduler Http4s Spec")(
      testM("Test jobs end point") {
        val body = Json.obj("query" -> Json.fromString("{jobs {name}}"))
        println(body.toString())
        val request = Request[EtlFlowTask](method = POST, uri = uri"/api/etlflow").withEntity(body)
        val response = client.expect[String](request).provideCustomLayer(env)
        assertM(response)(equalTo("""{"data":{"jobs":[{"name":"EtlJob5PARQUETtoJDBC"}]}}"""))
      }
    )
}
