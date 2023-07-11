package etlflow.task

import etlflow.http.{Http, HttpMethod}
import sttp.client3.Response
import zio._

/** Describes a HTTP request
  *
  * @param name
  *   Name of the Task
  * @param url
  *   Http Request URL
  * @param method
  *   Supported Http method are GET, POST, PUT
  * @param params
  *
  * For POST/PUT Requests: To encode http request body as JSON use Left(String), To encode http request body as FORM use
  * Right(Map[String, String])
  *
  * For GET Requests: To send params in URL use Right(Map[String, String]), Left(String) is not available for GET
  * @param headers
  *   Http request headers
  * @param log
  *   Boolean flag to enable/disable detailed logging of HTTP requests
  * @param connectionTimeout
  *   Http request connection timeout in MILLISECONDS
  * @param readTimeout
  *   Http request read timeout in MILLISECONDS
  * @param allowUnsafeSSL
  *   Allow sending unsafe SSL requests
  */
@SuppressWarnings(Array("org.wartremover.warts.ToString"))
case class HttpRequestTask(
    name: String,
    url: String,
    method: HttpMethod,
    params: Either[String, Map[String, String]] = Right(Map.empty),
    headers: Map[String, String] = Map.empty,
    log: Boolean = false,
    connectionTimeout: Long = 10000,
    readTimeout: Long = 150000,
    allowUnsafeSSL: Boolean = false
) extends EtlTask[Http, Response[String]] {

  override protected def process: RIO[Http, Response[String]] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Starting HttpRequestTask: $name")
    _ <- ZIO.logInfo(s"URL: $url")
    _ <- ZIO.logInfo(s"ConnectionTimeOut: $connectionTimeout")
    _ <- ZIO.logInfo(s"ReadTimeOut: $readTimeout")
    _ <- ZIO.logInfo(s"AllowUnsafeSSL: $allowUnsafeSSL")
    response <- Http
      .execute(method, url, params, headers, log, connectionTimeout, readTimeout, allowUnsafeSSL)
  } yield response

  override def getMetaData: Map[String, String] = Map("url" -> url, "method" -> method.toString)
}
