package etlflow.http

import etlflow.log.ApplicationLogger
import sttp.capabilities
import sttp.capabilities.zio.ZioStreams
import sttp.client3.httpclient.zio._
import sttp.client3.logging.LogLevel
import sttp.client3.logging.slf4j.Slf4jLoggingBackend
import sttp.client3._
import sttp.model.MediaType
import zio.{Scope, Task, ZIO}
import java.net.http.HttpClient
import java.security.SecureRandom
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import java.security.cert.X509Certificate
import javax.net.ssl.SSLContext
import scala.concurrent.duration._

private[etlflow] object HttpApi extends ApplicationLogger {

  private def logBackend(
      backend: SttpBackend[Task, ZioStreams with capabilities.WebSockets]
  ): SttpBackend[Task, ZioStreams with capabilities.WebSockets] =
    Slf4jLoggingBackend(
      backend,
      beforeCurlInsteadOfShow = true,
      logRequestBody = true,
      logResponseBody = true,
      beforeRequestSendLogLevel = LogLevel.Info,
      responseLogLevel = _ => LogLevel.Info
    )

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.NonUnitStatements"))
  private def getBackend(
      allowUnsafeSSL: Boolean,
      connectionTimeout: Int
  ): ZIO[Scope, Throwable, SttpBackend[Task, ZioStreams with capabilities.WebSockets]] =
    if (allowUnsafeSSL) {
      val trustAllCerts = Array[TrustManager](new X509TrustManager() {
        override def getAcceptedIssuers: Array[X509Certificate] = null

        override def checkClientTrusted(certs: Array[X509Certificate], authType: String): Unit = {}
        override def checkServerTrusted(certs: Array[X509Certificate], authType: String): Unit = {}
      })

      val sslContext: SSLContext = SSLContext.getInstance("ssl")
      sslContext.init(null, trustAllCerts, new SecureRandom())

      val client: HttpClient = HttpClient
        .newBuilder()
        .connectTimeout(java.time.Duration.ofMillis(connectionTimeout.toLong))
        .sslContext(sslContext)
        .build()

      val backend = HttpClientZioBackend.usingClient(client)
      ZIO.acquireRelease(ZIO.attempt(backend))(a => a.close().ignore)
    } else {
      val options = SttpBackendOptions.connectionTimeout(connectionTimeout.millisecond)
      HttpClientZioBackend.scoped(options)
    }

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  private def logAndParseResponse(
      req: RequestT[Identity, String, Any],
      log: Boolean,
      connectionTimeout: Int,
      allowUnsafeSsl: Boolean
  ): ZIO[Scope, Throwable, Response[String]] =
    getBackend(allowUnsafeSsl: Boolean, connectionTimeout: Int)
      .flatMap(backend => if (log) req.send(logBackend(backend)) else req.send(backend))
      .map { res =>
        logger.info("#" * 50)
        if (res.code.code == 204 || res.code.code == 200 || res.code.code == 201) {
          res
        } else {
          logger.error(s"Failed with Response code: ${res.code}")
          throw new RuntimeException(s"Failed with Response code: ${res.code}")
        }
      }

  implicit private val stringAsJson: BodySerializer[String] = { (p: String) =>
    StringBody(p, "UTF-8", MediaType.ApplicationJson)
  }

  def execute(
      method: HttpMethod,
      url: String,
      params: Either[String, Map[String, String]],
      headers: Map[String, String],
      log: Boolean,
      connectionTimeout: Int,
      readTimeout: Int,
      allowUnsafeSsl: Boolean = false
  ): Task[Response[String]] = {
    val hdrs = headers -- List("content-type", "Content-Type")

    val request: RequestT[Empty, String, Any] = method match {
      case HttpMethod.GET =>
        basicRequest
          .readTimeout(Duration(readTimeout.toLong, MILLISECONDS))
          .headers(headers)
          .response(asStringAlways)
      case HttpMethod.POST | HttpMethod.PUT =>
        params match {
          case Left(str) =>
            basicRequest
              .body(stringAsJson(str)) // Always encoded as JSON
              .readTimeout(Duration(readTimeout.toLong, MILLISECONDS))
              .headers(hdrs)
              .response(asStringAlways)
          case Right(map) =>
            basicRequest
              .body(map) // Always encoded as FORM
              .readTimeout(Duration(readTimeout.toLong, MILLISECONDS))
              .headers(hdrs)
              .response(asStringAlways)
        }
    }

    ZIO.scoped {
      method match {
        case HttpMethod.GET =>
          params match {
            case Left(_)    => ZIO.fail(new RuntimeException("params for get request as Left(..) is not supported"))
            case Right(map) => logAndParseResponse(request.get(uri"$url?$map"), log, connectionTimeout, allowUnsafeSsl)
          }
        case HttpMethod.POST => logAndParseResponse(request.post(uri"$url"), log, connectionTimeout, allowUnsafeSsl)
        case HttpMethod.PUT  => logAndParseResponse(request.put(uri"$url"), log, connectionTimeout, allowUnsafeSsl)
      }
    }
  }
}
