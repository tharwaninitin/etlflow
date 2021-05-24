package etlflow.webserver

import caliban.ZHttpAdapter
import etlflow.api.ServerTask
import etlflow.utils.WebServer
import etlflow.{BuildInfo => BI}
import zhttp.http._
import zhttp.service.Server
import zio.stream._
import etlflow.utils.GetCorsConfigZio
trait ZioHttpServer {

  val otherRoutes: HttpApp[Any, Nothing] = HttpApp.collect {
    case _ -> Root / "about" => Response.text(s"Hello, Welcome to EtlFlow API ${BI.version}, Build with scala version ${BI.scalaVersion}")
  }

  val staticRoutes = HttpApp.collect {
    case _ -> Root => Response.http(content = HttpData.fromStream(ZStream.fromResource("static/index.html")))
    case _ -> Root / "assets" / "js" / "2.70d81952.chunk.js" => Response.http(content = HttpData.fromStream(ZStream.fromResource("static/assets/js/2.70d81952.chunk.js")))
    case _ -> Root / "assets" / "js" / "main.9b2263d7.chunk.js" => Response.http(content = HttpData.fromStream(ZStream.fromResource("static/assets/js/main.9b2263d7.chunk.js")))
    case _ -> Root / "assets" / "css" / "2.f4ede277.chunk.css" => Response.http(content = HttpData.fromStream(ZStream.fromResource("static/assets/css/2.f4ede277.chunk.css")))
    case _ -> Root / "assets" / "css" / "main.2470ea74.chunk.css" => Response.http(content = HttpData.fromStream(ZStream.fromResource("static/assets/css/main.2470ea74.chunk.css")))
  }

  def etlFlowWebServer(auth: ZioAuthentication, config: Option[WebServer]) : ServerTask[Nothing] =
    (for {
      etlFlowInterpreter <- GqlAPI.api.interpreter
      loginInterpreter   <- GqlLoginAPI.api.interpreter
      corsConfig         = GetCorsConfigZio(config)
      port    = config.map(_.port.getOrElse(8080)).getOrElse(8080)

      _           <- Server
        .start(
          port,
                otherRoutes +++
                staticRoutes +++
                CORS(ZioWebsocketAPI(auth).webSocketApp) +++
                CORS(ZioRestAPI.oldRestApi) +++
                CORS(ZioRestAPI.newRestApi, corsConfig) +++
                Http.route {
                  case _ -> Root / "api" / "etlflow" => CORS(auth.middleware(ZHttpAdapter.makeHttpService(etlFlowInterpreter)))
                  case _ -> Root / "api" / "login" => CORS(ZHttpAdapter.makeHttpService(loginInterpreter))
                }
        ).forever
    } yield ()).forever
}
