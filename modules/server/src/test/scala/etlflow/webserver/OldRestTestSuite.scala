package etlflow.webserver

import zhttp.http._
import zhttp.service.server._
import zhttp.service.{ChannelFactory, Client, EventLoopGroup}
import zio.test.Assertion.equalTo
import zio.test.{ZSpec, environment, assertM}
import zio.test._

case class OldRestTestSuite(port: Int) extends HttpRunnableSpec(port) {

  val oldRestApi = serve {RestAPI.oldRestApi}

  val spec: ZSpec[environment.TestEnvironment with TestAuthEnv, Any] =
    suiteM("Old Rest Api")(
      oldRestApi
      .as(
        List(
          testM("500 response When incorrect job name is provided") {
            val actual = Client.request(s"http://localhost:$port/api/runjob?job_name=Job")
            assertM(actual.map(x => x.status))(equalTo(Status.INTERNAL_SERVER_ERROR))
          },
          testM("500 response When no job name is provided") {
            val actual = Client.request(s"http://localhost:$port/api/runjob")
            assertM(actual.map(x => x.status))(equalTo(Status.INTERNAL_SERVER_ERROR))
          },
//          testM("200 response when Job Run Successfully with props") {
//            val actual = Client.request("http://localhost:8080/api/runjob?job_name=Job1&props={\"bu\":\"ent\"}")
//            assertM(actual)(anything)
//          },
//          testM("404 response when incorrect path is provided") {
//            val actual = Client.request("""http://localhost:8080/api/runjob123?job_name=Job1&props=("bu":"ent")""")
//            assertM(actual)(anything)
//          }
        )
      )
      .useNow,
  )
}