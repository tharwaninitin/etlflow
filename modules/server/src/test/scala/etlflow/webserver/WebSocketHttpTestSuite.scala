package etlflow.webserver

import etlflow.ServerSuiteHelper
import pdi.jwt.{Jwt, JwtAlgorithm}
import zhttp.http.{Status, _}
import zhttp.service.server.ServerChannelFactory
import zio.test.assertCompletes
import zhttp.http._
import zhttp.service.{ChannelFactory, EventLoopGroup}
import zio.test.Assertion.equalTo
import zio.test.{ZSpec, assertM, environment}

object WebSocketHttpTestSuite extends HttpRunnableSpec(8082) {

  val env = EventLoopGroup.auto() ++ ChannelFactory.auto ++ ServerChannelFactory.auto ++ (testAPILayer ++ testDBLayer ++ testJsonLayer).orDie

  val wsApi = serve {WebsocketAPI(auth).webSocketApp}
  val token: String = Jwt.encode("""{"user":"test"}""", auth.secret, JwtAlgorithm.HS256)

  override def spec: ZSpec[environment.TestEnvironment, Any]  =
    suiteM("WebSocket Api")(
      wsApi
        .as(
          List(
            testM("NOT_FOUND response when Incorrect URL provided.") {
              val actual = statusGet(Root  / "ws" / "etlflow" )
              assertM(actual)(equalTo(Status.NOT_FOUND))
            },
            testM("200 response when valid URL provided.") {
              val actual = statusGet(Root  / "ws" / "etlflow" / token)
              actual.as(assertCompletes)
            }
          )
        ).useNow,
    ).provideCustomLayer(env)
}
