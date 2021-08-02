package etlflow.webserver

import zhttp.http._
import zhttp.service.server._
import zhttp.service.{ChannelFactory, EventLoopGroup}
import zio.test.Assertion.equalTo
import zio.test.{ZSpec, assertM, environment}

object NewRestTestSuite extends HttpRunnableSpec(8080) {

  val env = EventLoopGroup.auto() ++ ChannelFactory.auto ++ ServerChannelFactory.auto ++ (fullLayer).orDie

  val newRestApi = serve {RestAPI.newRestApi}

  override def spec: ZSpec[environment.TestEnvironment, Any]  =
    suiteM("New Rest Api")(
      newRestApi
      .as(
        List(
          testM("200 response when Job Run Successfully.") {
            val actual = statusPost(Root / "restapi" / "runjob" / "Job1",None)
            assertM(actual)(equalTo(Status.OK))
          },
          testM("500 response When incorrect job name is provided") {
            val actual = statusPost(Root / "restapi" / "runjob" / "Job",None)
            assertM(actual)(equalTo(Status.INTERNAL_SERVER_ERROR))
          },
          testM("500 response When job throws an exception") {
            val actual = statusPost(Root / "restapi" / "runjob" / "Job5",None )
            assertM(actual)(equalTo(Status.INTERNAL_SERVER_ERROR))
          },
          testM("404 response when incorrect path is provided") {
            val actual = statusPost(Root / "restapi" / "runjob123",None )
            assertM(actual)(equalTo(Status.NOT_FOUND))
          },
//          testM("post request with nonempty content") {
//            val path    = "postWithNonemptyContent"
//            val content = "content"
//            val actual  = request(Root / "restapi" / "runjob" / "Job1" , Method.POST, content)
//            assertM(actual)(equalTo(Response.ok))
//          },
        )
      )
      .useNow,
  ).provideCustomLayer(env)
}
