package etlflow.webserver

import etlflow.server.model.{EtlJobArgs, Props}
import etlflow.server.{ServerEnv, Service}
import etlflow.json.JsonApi
import io.circe.generic.auto._
import zhttp.http.Method._
import zhttp.http._

object RestAPI {
  def getProps(json: String): Option[List[Props]] = io.circe.parser.decode[Map[String, String]](json) match {
    case Left(_)      => None
    case Right(value) => Some(value.map(kv => Props(kv._1, kv._2)).toList)
  }

  def apply(): HttpApp[ServerEnv, Throwable] =
    Http.collectM { case req @ POST -> !! / "restapi" / "runjob" / name =>
      for {
        reqStr <- req.getBodyAsString
        props = getProps(reqStr)
        etlJob <- Service.runJob(EtlJobArgs(name, props), "Rest API")
        json   <- JsonApi.convertToString(etlJob, List.empty)
        response = Response.json(json)
      } yield response

    }
}
