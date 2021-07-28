package etlflow.webserver

import etlflow.ServerSuiteHelper
import etlflow.webserver.NewRestTestSuite.{serve, statusGet}
import pdi.jwt.{Jwt, JwtAlgorithm}
import zhttp.http.{Status, _}
import zhttp.service.server.ServerChannelFactory
import zhttp.service.{ChannelFactory, EventLoopGroup}
import zio.test.Assertion.equalTo
import zio.test.{ZSpec, assertM, environment, _}

object WebSocketHttpTestSuite extends DefaultRunnableSpec with ServerSuiteHelper {

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
            }
          )
        )
        .useNow,
    ).provideCustomLayer(env)
}
