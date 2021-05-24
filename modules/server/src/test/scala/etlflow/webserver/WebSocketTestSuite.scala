//package etlflow.webserver
//
//import etlflow.ServerSuiteHelper
//import etlflow.utils.CacheHelper
//import pdi.jwt.{Jwt, JwtAlgorithm}
//import zio.interop.catz._
//import zio.test.Assertion.containsString
//import zio.test._
//import zio.{Task, UIO}
//import java.nio.charset.StandardCharsets.UTF_8
//
//object WebSocketTestSuite extends DefaultRunnableSpec with ServerSuiteHelper {
//
//  val ws: WebsocketAPI = WebsocketAPI(auth)
//  val token: String = Jwt.encode("""{"user":"test"}""", auth.secret, JwtAlgorithm.HS256)
//  def testStream(token: String): Task[String] = ws.websocketStream(token)
//      .evalTap(x => UIO(println(x)))
//      .head
//      .compile
//      .toList
//      .map(l => l.head)
//      .map(wsf => new String(wsf.data.toArray, UTF_8))
//
//  /*
//  TESTING CASE 1 => Invalid Token
//    ws = new WebSocket('ws://localhost:8080/ws/etlflow/abcd')
//    ws.onclose = function(e) { console.error(e) }
//  TESTING CASE 2 => Expired token
//    ws = new WebSocket('ws://localhost:8080/ws/etlflow/xxxxx')
//    ws.onclose = function(e) { console.error(e) }
//  TESTING CASE 3 => Correct token
//    ws = new WebSocket('ws://localhost:8080/ws/etlflow/xxxxx')
//    ws.onclose = function(e) { console.error(e) }
//    ws.onmessage = function(e) { console.info(e) }
//    ws.close()
//  */
//
//  override def spec: ZSpec[environment.TestEnvironment, Any] = {
//    suite("WebSocket Suite")(
//      testM("Test WebSocketFrame stream with invalid token") {
//        assertM(testStream("abcd.xxx.123"))(containsString("Invalid token"))
//      },
//      testM("Test WebSocketFrame stream with expired token") {
//        assertM(testStream(token))(containsString("Expired token"))
//      },
//      testM("Test WebSocketFrame stream with correct token") {
//        CacheHelper.putKey(authCache, token, token, Some(CacheHelper.default_ttl))
//        assertM(testStream(token))(containsString("memory"))
//      },
//    ) @@ TestAspect.sequential
//  }
//}
