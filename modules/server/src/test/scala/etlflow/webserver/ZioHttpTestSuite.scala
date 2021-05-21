//package etlflow.webserver
//
//import etlflow.ServerSuiteHelper
//import etlflow.utils.CacheHelper
//import zhttp.http._
//import zhttp.service.server._
//import zhttp.service.{ChannelFactory, EventLoopGroup}
//import zio.Task
//import zio.test.Assertion.equalTo
//import zio.test.assertM
//
//object ZioHttpTestSuite extends ZioHttpRunnableSpec(8083) with ZioHttpServer with ServerSuiteHelper {
//
//  val env = EventLoopGroup.auto() ++ ChannelFactory.auto ++ ServerChannelFactory.auto ++ (testAPILayer ++ testDBLayer).orDie
//
//  val serverApp =
//    for {
//      _           <- Task(100)
//      authCache   = CacheHelper.createCache[String]
//      _           = config.token.map(_.foreach(tkn => CacheHelper.putKey(authCache,tkn,tkn)))
//      auth        = ZioAuthentication(authCache, config.webserver)
//       graphqlEndPoint    <- graphqlEndPoint(auth)
//    } yield graphqlEndPoint
//
//
//  override def spec = suiteM("Client Content-Length auto assign")(
//    serverApp
//      .as(
//        List(
//          testM("post request with nonempty content and set content-length") {
//            val path    = "api/etlflow"
//            val content = "{jobs {name}"
//            val actual  = request(Root / path, Method.POST, content)
//            assertM(actual.map(_.content))(equalTo(HttpData.empty))
//          },
//        ),
//      )
//  ).provideCustomLayer(env)
//}