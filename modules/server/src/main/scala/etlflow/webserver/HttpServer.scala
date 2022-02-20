package etlflow.webserver

import etlflow.server.ServerTask
import etlflow.model.WebServer
import etlflow.webserver.middleware.CORSConfig
import etlflow.{BuildInfo => BI}
import zio._
import zio.stream._
import zhttp.http._
import zhttp.http.Middleware.cors
import zhttp.service.Server
import zio.blocking.Blocking

trait HttpServer {
  def etlFlowWebServer(auth: Authentication, config: Option[WebServer]): ServerTask[Nothing] =
    for {
      corsConfig <- UIO(CORSConfig(config))
      port = config.map(_.port.getOrElse(8080)).getOrElse(8080)
      app1 = Http.collect[Request] {
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
      app2 = Http.route[Request] {
        case _ -> !! / "api" / "login"       => RestAPI.live
        case _ -> !! / "api" / "etlflow" / _ => auth.middleware(RestAPI.live) @@ cors(corsConfig)
        case _ -> !! / "ws" / "etlflow" / _  => WebsocketsAPI(auth).webSocketApp @@ cors()
      }
      app = app1 <> app2
      op <- Server.start(port, app)
    } yield op
}
