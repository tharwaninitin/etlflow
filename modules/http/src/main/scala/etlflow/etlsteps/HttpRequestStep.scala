package etlflow.etlsteps

import etlflow.json.{JsonApi, JsonEnv}
import etlflow.schema.LoggingLevel
import etlflow.utils.{HttpMethod, HttpRequest}
import io.circe.Decoder
import sttp.client3.Response
import zio._

import scala.reflect.runtime.universe.{TypeTag, typeOf}

case class HttpRequestStep[A: TypeTag : Decoder](
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

    val output: Task[Response[String]] = HttpRequest.execute(method, url, params, headers, log, connection_timeout, read_timeout, allow_unsafe_ssl)

    typeOf[A] match {
      case t if t =:= typeOf[Unit] =>
        output.unit.asInstanceOf[Task[A]]
      case t if t =:= typeOf[String] =>
        output.map(_.body).asInstanceOf[Task[A]]
      case t if t =:= typeOf[Nothing] =>
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
