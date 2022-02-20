package etlflow.webserver

import zhttp.http._
import zio.test.Assertion.equalTo
import zio.test._

case class RestTestSuite(port: Int) extends HttpRunnableSpec(port) {

  val restApi = serve(RestAPI.live)

  val spec: ZSpec[environment.TestEnvironment with TestAuthEnv, Any] =
    suiteM("Rest Api")(
      restApi
        .as(
          List(
            testM("200 response when correct job name is provided.") {
              val actual = statusPost(!! / "api" / "etlflow" / "runjob" / "Job1")
              assertM(actual)(equalTo(Status.OK))
            },
            testM("200 response when valid data provided.") {
              val path = !! / "api" / "etlflow" / "runjob" / "Job1"
              val body = """{"key1":"value1"}"""
              val req  = request(path, Headers.empty, Method.POST, body)
              assertM(req.map(_.status))(equalTo(Status.OK))
            },
            testM("500 response when incorrect job name is provided") {
              val actual = statusPost(!! / "api" / "etlflow" / "runjob" / "Job")
              assertM(actual)(equalTo(Status.INTERNAL_SERVER_ERROR))
            },
            testM("500 response when job throws an exception") {
              val actual = statusPost(!! / "api" / "etlflow" / "runjob" / "Job5")
              assertM(actual)(equalTo(Status.INTERNAL_SERVER_ERROR))
            },
            testM("404 response when incorrect path is provided") {
              val actual = statusPost(!! / "api" / "runjob123")
              assertM(actual)(equalTo(Status.NOT_FOUND))
            }
          )
        )
        .useNow
    )
}
