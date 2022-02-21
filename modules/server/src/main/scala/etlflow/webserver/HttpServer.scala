package etlflow.webserver

import etlflow.server.ServerTask
import etlflow.model.WebServer
import etlflow.webserver.middleware.CORSConfig
import etlflow.{BuildInfo => BI}
import zio.stream._
import zhttp.http._
import zhttp.service.Server
import zio.blocking.Blocking

object HttpServer {
  def apply(auth: Authentication, config: Option[WebServer]): ServerTask[Nothing] = {
    val port    = config.map(_.port.getOrElse(8080)).getOrElse(8080)
    val cconfig = CORSConfig(config)
    val app1 = Http.collect[Request] {
      case _ -> !! / "about" =>
        Response.text(s"Hello, Welcome to EtlFlow API ${BI.version}, Build with scala version ${BI.scalaVersion}")
      case _ -> !! =>
        Response(data = HttpData.fromStream(ZStream.fromResource("static/index.html").provideLayer(Blocking.live)))
      case _ -> !! / "assets" / "js" / "2.70d81952.chunk.js" =>
        Response(data =
          HttpData.fromStream(ZStream.fromResource("static/assets/js/2.70d81952.chunk.js").provideLayer(Blocking.live))
        )
      case _ -> !! / "assets" / "js" / "main.9b2263d7.chunk.js" =>
        Response(data =
          HttpData.fromStream(ZStream.fromResource("static/assets/js/main.9b2263d7.chunk.js").provideLayer(Blocking.live))
        )
      case _ -> !! / "assets" / "css" / "2.f4ede277.chunk.css" =>
        Response(data =
          HttpData.fromStream(ZStream.fromResource("static/assets/css/2.f4ede277.chunk.css").provideLayer(Blocking.live))
        )
      case _ -> !! / "assets" / "css" / "main.2470ea74.chunk.css" =>
        Response(data =
          HttpData.fromStream(ZStream.fromResource("static/assets/css/main.2470ea74.chunk.css").provideLayer(Blocking.live))
        )
    }
    val app2 = RestAPI.login
    val app3 = auth.middleware(RestAPI.live) @@ Middleware.cors(cconfig)
    val app4 = WebsocketsAPI(auth).webSocketApp @@ Middleware.cors()
    val app  = app1 ++ app2 ++ app3 ++ app4
    Server.start(port, app)
  }
}
