package etlflow.webserver

import zhttp.http._
import zio.test.Assertion.equalTo
import zio.test._

case class RestTestSuite(port: Int) extends HttpRunnableSpec(port) {

  val newRestApi = serve(RestAPI.live)

  val spec: ZSpec[environment.TestEnvironment with TestAuthEnv, Any] =
    suiteM("Rest Api")(
      newRestApi
        .as(
          List(
            testM("200 response when Job Run Successfully.") {
              val actual = statusPost(!! / "restapi" / "runjob" / "Job1")
              assertM(actual)(equalTo(Status.OK))
            },
            testM("500 response When incorrect job name is provided") {
              val actual = statusPost(!! / "restapi" / "runjob" / "Job")
              assertM(actual)(equalTo(Status.INTERNAL_SERVER_ERROR))
            },
            testM("500 response When job throws an exception") {
              val actual = statusPost(!! / "restapi" / "runjob" / "Job5")
              assertM(actual)(equalTo(Status.INTERNAL_SERVER_ERROR))
            },
            testM("404 response when incorrect path is provided") {
              val actual = statusPost(!! / "restapi" / "runjob123")
              assertM(actual)(equalTo(Status.NOT_FOUND))
            }
          )
        )
        .useNow
    )
}
