package etlflow.etlsteps

import etlflow.http.{HttpMethod, HttpApi}
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.schema.LoggingLevel
import io.circe.Decoder
import sttp.client3.Response
import zio._

case class HttpRequestStep[A: Tag : Decoder](
     name: String,
     url: String,
     method: HttpMethod,
     params: Either[String, Map[String,String]] = Right(Map.empty),
     headers: Map[String,String] = Map.empty,
     log: Boolean = false,
     connection_timeout: Int = 10000,
     read_timeout: Int = 150000,
     allow_unsafe_ssl: Boolean = false
   )
  extends EtlStep[Unit, A] {

  final def process(in: =>Unit): RIO[JsonEnv, A] = {
    logger.info("#"*100)
    logger.info(s"Starting HttpRequestStep: $name")
    logger.info(s"URL: $url")
    logger.info(s"ConnectionTimeOut: $connection_timeout")
    logger.info(s"ReadTimeOut: $read_timeout")

    val output: Task[Response[String]] = HttpApi.execute(method, url, params, headers, log, connection_timeout, read_timeout, allow_unsafe_ssl)

    Tag[A] match {
      case t if t == Tag[Unit] =>
        output.unit.asInstanceOf[Task[A]]
      case t if t == Tag[String] =>
        output.map(_.body).asInstanceOf[Task[A]]
      case t if t == Tag[Nothing] =>
        Task.fail(new RuntimeException("Need type parameter in HttpStep, if no output is required use HttpStep[Unit]"))
      case _ =>
        for {
          op <- output
          obj <- JsonApi.convertToObject[A](op.body)
        } yield obj
    }
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map(
      "url" -> url,
      "http_method" -> method.toString
    )
}
