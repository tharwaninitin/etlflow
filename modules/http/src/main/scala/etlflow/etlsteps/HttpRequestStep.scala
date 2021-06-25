package etlflow.etlsteps

import etlflow.json.{Implementation, JsonService}
import etlflow.utils.{HttpMethod, HttpRequest, LoggingLevel}
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

  final def process(in: =>Unit): Task[A] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting HttpRequestStep: $name")
    etl_logger.info(s"URL: $url")
    etl_logger.info(s"ConnectionTimeOut: $connection_timeout")
    etl_logger.info(s"ReadTimeOut: $read_timeout")

    val output: Task[Response[String]] = HttpRequest.execute(method, url, params, headers, log, connection_timeout, read_timeout, allow_unsafe_ssl)

    typeOf[A] match {
      case t if t =:= typeOf[Unit] =>
        output.unit.asInstanceOf[Task[A]]
      case t if t =:= typeOf[String] =>
        output.map(_.body).asInstanceOf[Task[A]]
      case t if t =:= typeOf[Nothing] =>
        Task.fail(new RuntimeException("Need type parameter in HttpStep, if no output is required use HttpStep[Unit]"))
      case _ => {
        for {
          op <- output
          obj <- JsonService.convertToObject[A](op.body).provideLayer(Implementation.live)
        } yield obj
      }
    }
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = Map(
      "url" -> url,
      "http_method" -> method.toString
    )
}
