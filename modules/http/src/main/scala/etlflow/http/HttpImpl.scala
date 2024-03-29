package etlflow.http
import etlflow.log.ApplicationLogger
import sttp.capabilities
import sttp.capabilities.zio.ZioStreams
import sttp.client3._
import sttp.client3.httpclient.zio._
import sttp.client3.logging.LogLevel
import sttp.client3.logging.slf4j.Slf4jLoggingBackend
import sttp.model.MediaType
import zio.{Scope, Task, ZIO}
import java.net.http.HttpClient
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.{SSLContext, TrustManager, X509TrustManager}
import scala.concurrent.duration._

object HttpImpl extends Http with ApplicationLogger {

  type EtlFlowSttpBackend = SttpBackend[Task, ZioStreams with capabilities.WebSockets]

  /** @param backend
    * @param logDetails
    * @return
    */
  def logBackend(backend: EtlFlowSttpBackend, logDetails: Boolean): EtlFlowSttpBackend = Slf4jLoggingBackend(
    backend,
    logRequestBody = logDetails,
    logRequestHeaders = logDetails,
    logResponseBody = logDetails,
    logResponseHeaders = logDetails,
    beforeCurlInsteadOfShow = logDetails,
    beforeRequestSendLogLevel = LogLevel.Info,
    responseLogLevel = c => if (c.isClientError || c.isServerError) LogLevel.Error else LogLevel.Info
  )

  def getBackendWithSSLContext(
      connectionTimeout: Long,
      sslContext: SSLContext,
      followRedirects: HttpClient.Redirect = HttpClient.Redirect.NORMAL
  ): ZIO[Scope, Throwable, EtlFlowSttpBackend] = {
    val client: HttpClient = HttpClient
      .newBuilder()
      .connectTimeout(java.time.Duration.ofMillis(connectionTimeout))
      .followRedirects(followRedirects)
      .sslContext(sslContext)
      .build()

    val backend = HttpClientZioBackend.usingClient(client)
    ZIO.acquireRelease(ZIO.attempt(backend))(a => a.close().ignore)
  }

  /** @param connectionTimeout
    * @param allowUnsafeSSL
    * @return
    */
  // noinspection ScalaStyle
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def getBackend(connectionTimeout: Long, allowUnsafeSSL: Boolean): ZIO[Scope, Throwable, EtlFlowSttpBackend] =
    if (allowUnsafeSSL) {
      val trustAllCerts = Array[TrustManager](new X509TrustManager() {
        // noinspection ScalaStyle
        override def getAcceptedIssuers: Array[X509Certificate]                                = null
        override def checkClientTrusted(certs: Array[X509Certificate], authType: String): Unit = {}
        override def checkServerTrusted(certs: Array[X509Certificate], authType: String): Unit = {}
      })

      val sslContext: SSLContext = SSLContext.getInstance("ssl")
      sslContext.init(null, trustAllCerts, new SecureRandom())

      getBackendWithSSLContext(connectionTimeout, sslContext)
    } else {
      val options = SttpBackendOptions.connectionTimeout(connectionTimeout.millisecond)
      HttpClientZioBackend.scoped(options)
    }

  /** @param req
    * @param logDetails
    * @param connectionTimeout
    * @param allowUnsafeSsl
    * @return
    */
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  def logAndParseResponse(
      req: RequestT[Identity, String, Any],
      logDetails: Boolean,
      connectionTimeout: Long,
      allowUnsafeSsl: Boolean
  ): ZIO[Scope, Throwable, Response[String]] =
    getBackend(connectionTimeout, allowUnsafeSsl)
      .flatMap(backend => if (logDetails) req.send(logBackend(backend, logDetails)) else req.send(backend))
      .map { res =>
        if (res.code.code == 204 || res.code.code == 200 || res.code.code == 201) {
          logger.info(s"Response code: ${res.code}")
          res
        } else {
          logger.error(s"Failed with Response code: ${res.code}")
          throw new RuntimeException(s"Failed with Response code: ${res.code}")
        }
      }
  implicit private val stringAsJson: BodySerializer[String] = { (p: String) =>
    StringBody(p, "UTF-8", MediaType.ApplicationJson)
  }

  /** @param method
    * @param url
    * @param params
    * @param headers
    * @param logDetails
    * @param connectionTimeout
    * @param readTimeout
    * @param allowUnsafeSsl
    * @return
    */
  override def execute(
      method: HttpMethod,
      url: String,
      params: Either[String, Map[String, String]],
      headers: Map[String, String],
      logDetails: Boolean,
      connectionTimeout: Long,
      readTimeout: Long,
      allowUnsafeSsl: Boolean
  ): Task[Response[String]] = {
    val hdrs = headers -- List("content-type", "Content-Type")

    val request: PartialRequest[String, Any] = method match {
      case HttpMethod.GET =>
        basicRequest
          .readTimeout(Duration(readTimeout, MILLISECONDS))
          .headers(headers)
          .response(asStringAlways)
      case HttpMethod.POST | HttpMethod.PUT =>
        params match {
          case Left(str) =>
            basicRequest
              .body(stringAsJson(str)) // Always encoded as JSON
              .readTimeout(Duration(readTimeout, MILLISECONDS))
              .headers(hdrs)
              .response(asStringAlways)
          case Right(map) =>
            basicRequest
              .body(map) // Always encoded as FORM
              .readTimeout(Duration(readTimeout, MILLISECONDS))
              .headers(hdrs)
              .response(asStringAlways)
        }
    }

    ZIO.scoped {
      method match {
        case HttpMethod.GET =>
          params match {
            case Left(_)    => ZIO.fail(new RuntimeException("params passed for GET Request as Left(..) is not available"))
            case Right(map) => logAndParseResponse(request.get(uri"$url?$map"), logDetails, connectionTimeout, allowUnsafeSsl)
          }
        case HttpMethod.POST => logAndParseResponse(request.post(uri"$url"), logDetails, connectionTimeout, allowUnsafeSsl)
        case HttpMethod.PUT  => logAndParseResponse(request.put(uri"$url"), logDetails, connectionTimeout, allowUnsafeSsl)
      }
    }
  }
}
