package etlflow.webserver

import etlflow.ServerSuiteHelper
import pdi.jwt.{Jwt, JwtAlgorithm}
import zio.clock.Clock
import zio.test._
import zio.{UIO, ZIO}

object WebSocketApiTestSuite extends DefaultRunnableSpec with ServerSuiteHelper {

  val ws: WebsocketAPI = WebsocketAPI(auth)
  val token: String = Jwt.encode("""{"user":"test"}""", auth.secret, JwtAlgorithm.HS256)

  def testStream(token: String): ZIO[Any with Clock, Nothing, Unit] = ws.websocketStream(token)
    .mapM { producerRecord => UIO(println(producerRecord))}
    .runDrain

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("WebSocketApi Test Suite")(
      testM("WebSocketApi Test: InValid Login")(
        testStream("").as(assertCompletes)
      ),
      testM("WebSocketApi Test: Valid Login")(
        testStream(token).as(assertCompletes)
    )
    )@@ TestAspect.sequential)
}
