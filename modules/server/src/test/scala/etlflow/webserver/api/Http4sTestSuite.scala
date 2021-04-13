package etlflow.webserver.api

import cats.effect.Blocker
import etlflow.{ServerSuiteHelper, TestApiImplementation}
import etlflow.utils.EtlFlowHelper.EtlFlowTask
import etlflow.webserver.Http4sServer
import io.circe.Json
import io.circe.parser._
import org.http4s._
import org.http4s.circe._
import org.http4s.client.Client
import org.http4s.implicits._
import zio.blocking.Blocking
import zio.interop.catz._
import zio.test.Assertion.equalTo
import zio.test._
import zio.{Task, ZEnv, ZIO}

object Http4sTestSuite extends DefaultRunnableSpec with TestApiImplementation with Http4sServer with ServerSuiteHelper {

  zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))

  private val routesManaged = for {
    blocker     <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
    transactor  <- createDbTransactorManaged(config.dbLog, platform.executor.asEC, "Test-Pool")(blocker)
    routes      <- allRoutes[MEJP](blocker, cache, testJobsSemaphore, transactor, etlJob_name_package, testJobsQueue, config)
  } yield routes

  private def apiResponse(apiRequest: Request[EtlFlowTask]):ZIO[ZEnv, Throwable, Either[String, String]] = routesManaged.use {routes =>
    for {
      client <- Task(Client.fromHttpApp[EtlFlowTask](routes.orNotFound))
      output <- client.run(apiRequest).use {
                  case Status.Successful(r) => r.attemptAs[String].leftMap(_.message).value
                  case r => r.as[String].map(b => Left(s"${r.status.code}"))
                }
    } yield output
  }.provideCustomLayer(testHttp4s(transactor,cache))
    .fold(ex => Left(s"Status Code 500 with error ${ex.getMessage}"), op => op)

  private val gqlApiBody = Json.obj("query" -> Json.fromString("{jobs {name}}"))
  private val gqlLoginBody = Json.obj("query" -> Json.fromString("""
                mutation
                {
                  login(user_name:"admin",password:"admin"){
                   token
                  }
                }"""))

  private def apiResponseWithLogin(apiRequest: String => Request[EtlFlowTask]):ZIO[ZEnv, Throwable, Either[String, String]] = {
    for {
      jsonOutput  <- apiResponse(Request[EtlFlowTask](method = POST, uri = uri"/api/login").withEntity(gqlLoginBody))
      authToken   = parse(jsonOutput.getOrElse("")).getOrElse(Json.Null).hcursor.downField("data").downField("login").downField("token").as[String].getOrElse("")
      apiOutput   <- apiResponse(apiRequest(authToken))
    } yield apiOutput
  }

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Http4s Test Suite")(
      testM("Test GRAPHQL jobs end point with correct authorization header") {
        def apiRequest(authToken: String): Request[EtlFlowTask] = Request[EtlFlowTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("Authorization",authToken))).withEntity(gqlApiBody)
        assertM(apiResponseWithLogin(apiRequest))(equalTo(Right("""{"data":{"jobs":[{"name":"Job1"},{"name":"Job2"}]}}""")))
      },
      testM("Test GRAPHQL jobs end point with blank authorization header") {
        val apiRequest = Request[EtlFlowTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("Authorization",""))).withEntity(gqlApiBody)
        assertM(apiResponse(apiRequest))(equalTo(Left("403")))
      },
      testM("Test GRAPHQL jobs end point with incorrect authorization header") {
        val apiRequest = Request[EtlFlowTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("Authorization","abcd"))).withEntity(gqlApiBody)
        assertM(apiResponse(apiRequest))(equalTo(Left("403")))
      },
      testM("Test GRAPHQL jobs end point without authorization header") {
        val apiRequest = Request[EtlFlowTask](method = POST, uri = uri"/api/etlflow").withEntity(gqlApiBody)
        assertM(apiResponse(apiRequest))(equalTo(Left("403")))
      },
      testM("Test REST runjob end point with correct job name") {
        def apiRequest(authToken: String): Request[EtlFlowTask] = Request[EtlFlowTask](method = GET, uri = uri"/api/runjob?job_name=Job1", headers = Headers.of(Header("Authorization",authToken)))
        assertM(apiResponseWithLogin(apiRequest))(equalTo(Right("""{"message":"Job Job1 submitted successfully"}""")))
      },
      testM("Test REST runjob end point with no job name") {
        def apiRequest(authToken: String): Request[EtlFlowTask] = Request[EtlFlowTask](method = GET, uri = uri"/api/runjob?job_name=InvalidJob", headers = Headers.of(Header("Authorization",authToken)))
        assertM(apiResponseWithLogin(apiRequest))(equalTo(Left("Status Code 500 with error key not found: InvalidJob")))
      }
    ) @@ TestAspect.sequential
}
