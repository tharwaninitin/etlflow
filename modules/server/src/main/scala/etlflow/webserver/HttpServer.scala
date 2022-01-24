package etlflow.webserver

import caliban.ZHttpAdapter
import etlflow.server.ServerTask
import etlflow.model.WebServer
import etlflow.utils.CorsConfig
import etlflow.{BuildInfo => BI}
import zhttp.http.Middleware.cors
import zhttp.http._
import zhttp.service.Server
import zio.stream._

trait HttpServer {

  def etlFlowWebServer(auth: Authentication, config: Option[WebServer]): ServerTask[Nothing] =
    for {
      etlFlowInterpreter <- GqlAPI.api.interpreter
      loginInterpreter   <- GqlLoginAPI.api.interpreter
      corsConfig = CorsConfig(config)
      port       = config.map(_.port.getOrElse(8080)).getOrElse(8080)
      app1 = Http.collect[Request] {
        case _ -> !! / "about" =>
          Response.text(s"Hello, Welcome to EtlFlow API ${BI.version}, Build with scala version ${BI.scalaVersion}")
        case _ -> !! => Response(data = HttpData.fromStream(ZStream.fromResource("static/index.html")))
        case _ -> !! / "assets" / "js" / "2.70d81952.chunk.js" =>
          Response(data = HttpData.fromStream(ZStream.fromResource("static/assets/js/2.70d81952.chunk.js")))
        case _ -> !! / "assets" / "js" / "main.9b2263d7.chunk.js" =>
          Response(data = HttpData.fromStream(ZStream.fromResource("static/assets/js/main.9b2263d7.chunk.js")))
        case _ -> !! / "assets" / "css" / "2.f4ede277.chunk.css" =>
          Response(data = HttpData.fromStream(ZStream.fromResource("static/assets/css/2.f4ede277.chunk.css")))
        case _ -> !! / "assets" / "css" / "main.2470ea74.chunk.css" =>
          Response(data = HttpData.fromStream(ZStream.fromResource("static/assets/css/main.2470ea74.chunk.css")))
      }
      app2 = Http.route[Request] {
        case _ -> !! / "api" / "etlflow" => auth.middleware(ZHttpAdapter.makeHttpService(etlFlowInterpreter)) @@ cors()
        case _ -> !! / "api" / "login"   => ZHttpAdapter.makeHttpService(loginInterpreter) @@ cors()
        case _ -> !! / "ws" / "etlflow" / _               => WebsocketAPI(auth).webSocketApp @@ cors()
        case Method.POST -> !! / "restapi" / "runjob" / _ => auth.middleware(RestAPI()) @@ cors(corsConfig)
      }
      app = app1 <> app2
      op <- Server.start(port, app)
    } yield op
}
