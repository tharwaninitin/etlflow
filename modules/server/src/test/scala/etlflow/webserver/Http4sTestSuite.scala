//package etlflow.webserver
//
//import etlflow.ServerSuiteHelper
//import etlflow.api.ServerTask
//import etlflow.utils.GetCorsConfig
//import io.circe.Json
//import io.circe.parser._
//import org.http4s._
//import org.http4s.circe._
//import org.http4s.client.Client
//import org.http4s.implicits._
//import zio.Task
//import zio.interop.catz._
//import zio.test.Assertion.equalTo
//import zio.test._
//
//object Http4sTestSuite extends DefaultRunnableSpec with Http4sServer with ServerSuiteHelper {
//
//  zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))
//  val env = (testAPILayer ++ testDBLayer).orDie
//
//  private def apiResponse(apiRequest: Request[ServerTask]): ServerTask[Either[String, String]] = allRoutes[MEJP](auth,config.webserver).use { routes =>
//    for {
//      client <- Task(Client.fromHttpApp[ServerTask](routes.orNotFound))
//      output <- client.run(apiRequest).use {
//        case Status.Successful(r) => r.attemptAs[String].leftMap(_.message).value
//        case r => r.as[String].map(b => Left(s"${r.status.code}"))
//      }
//    } yield output
//  }.fold(ex => Left(s"Status Code 500 with error ${ex.getMessage}"), op => op)
//  private val gqlApiBody = Json.obj("query" -> Json.fromString("{jobs {name}}"))
//  private val gqlLoginBody = Json.obj("query" -> Json.fromString("""
//                mutation
//                {
//                  login(user_name:"admin",password:"admin"){
//                   token
//                  }
//                }"""))
//  private def apiResponseWithLogin(apiRequest: String => Request[ServerTask]): ServerTask[Either[String, String]] = {
//    for {
//      jsonOutput  <- apiResponse(Request[ServerTask](method = POST, uri = uri"/api/login").withEntity(gqlLoginBody))
//      authToken   = parse(jsonOutput.getOrElse("")).getOrElse(Json.Null).hcursor.downField("data").downField("login").downField("token").as[String].getOrElse("")
//      apiOutput   <- apiResponse(apiRequest(authToken))
//    } yield apiOutput
//  }
//
//  override def spec: ZSpec[environment.TestEnvironment, Any] =
//    (suite("Http4s Test Suite")(
//      testM("Test GRAPHQL jobs end point with correct authorization header") {
//        def apiRequest(authToken: String): Request[ServerTask] = Request[ServerTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("Authorization",authToken))).withEntity(gqlApiBody)
//        assertM(apiResponseWithLogin(apiRequest))(equalTo(Right("""{"data":{"jobs":[{"name":"Job1"},{"name":"Job2"}]}}""")))
//      },
//      testM("Test GRAPHQL jobs end point with correct X-Auth-Token header") {
//        def apiRequest(authToken: String): Request[ServerTask] = Request[ServerTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("X-Auth-Token",authToken))).withEntity(gqlApiBody)
//        assertM(apiResponseWithLogin(apiRequest))(equalTo(Right("""{"data":{"jobs":[{"name":"Job1"},{"name":"Job2"}]}}""")))
//      },
//      testM("Test GRAPHQL jobs end point with blank authorization header") {
//        val apiRequest = Request[ServerTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("Authorization",""))).withEntity(gqlApiBody)
//        assertM(apiResponse(apiRequest))(equalTo(Left("403")))
//      },
//      testM("Test GRAPHQL jobs end point with blank X-Auth-Token header") {
//        val apiRequest = Request[ServerTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("X-Auth-Token",""))).withEntity(gqlApiBody)
//        assertM(apiResponse(apiRequest))(equalTo(Left("403")))
//      },
//      testM("Test GRAPHQL jobs end point with incorrect authorization header") {
//        val apiRequest = Request[ServerTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("Authorization","abcd"))).withEntity(gqlApiBody)
//        assertM(apiResponse(apiRequest))(equalTo(Left("403")))
//      },
//      testM("Test GRAPHQL jobs end point with incorrect X-Auth-Token header") {
//        val apiRequest = Request[ServerTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("X-Auth-Token","abcd"))).withEntity(gqlApiBody)
//        assertM(apiResponse(apiRequest))(equalTo(Left("403")))
//      },
//      testM("Test GRAPHQL jobs end point without authorization header") {
//        val apiRequest = Request[ServerTask](method = POST, uri = uri"/api/etlflow").withEntity(gqlApiBody)
//        assertM(apiResponse(apiRequest))(equalTo(Left("403")))
//      },
//      testM("Test REST runjob end point with correct job name") {
//        def apiRequest(authToken: String): Request[ServerTask] = Request[ServerTask](method = GET, uri = uri"/api/runjob?job_name=Job1", headers = Headers.of(Header("Authorization",authToken)))
//        assertM(apiResponseWithLogin(apiRequest))(equalTo(Right("""{"message":"Job Job1 submitted successfully"}""")))
//      },
//      testM("Test REST runjob end point with no job name") {
//        def apiRequest(authToken: String): Request[ServerTask] = Request[ServerTask](method = GET, uri = uri"/api/runjob?job_name=InvalidJob", headers = Headers.of(Header("Authorization",authToken)))
//        assertM(apiResponseWithLogin(apiRequest))(equalTo(Left("Status Code 500 with error InvalidJob not present")))
//      }
//    ) @@ TestAspect.sequential).provideCustomLayerShared(env)
//}
