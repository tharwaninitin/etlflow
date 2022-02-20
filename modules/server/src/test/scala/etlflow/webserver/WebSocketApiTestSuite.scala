package etlflow.webserver

import pdi.jwt.{Jwt, JwtAlgorithm}
import zio.clock.Clock
import zio.test._
import zio.{UIO, ZIO}

case class WebSocketApiTestSuite(auth: Authentication) {

  val ws: WebsocketsAPI = WebsocketsAPI(auth)
  val token: String     = Jwt.encode("""{"user":"test"}""", auth.secret, JwtAlgorithm.HS256)

  def testStream(token: String): ZIO[Any with Clock, Nothing, Unit] =
    ws.webSocketStream(token).mapM(producerRecord => UIO(println(producerRecord))).runDrain

  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("WebSocket Api")(
      testM("WebSocketApi Test: InValid Login")(
        testStream("").as(assertCompletes)
      ),
      testM("WebSocketApi Test: Valid Login")(
        testStream(token).as(assertCompletes)
      )
    ) @@ TestAspect.sequential
}
