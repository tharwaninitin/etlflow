package etlflow.webserver

import etlflow.api.Schema.{EtlJobArgs, Props}
import etlflow.api.{ServerEnv, Service}
import etlflow.json.JsonApi
import io.circe.generic.auto._
import zhttp.http.Method._
import zhttp.http._

object RestAPI {
  def apply(): HttpApp[ServerEnv, Throwable] =
    HttpApp.collectM {
    case req@POST  -> Root /  "restapi" / "runjob" / name =>
      val props = io.circe.parser.decode[Map[String, String]](req.getBodyAsString.getOrElse("")) match {
        case Left(_) => None
        case Right(value) => Some(value.map(kv => Props(kv._1,kv._2)).toList)
      }
      for {
          etlJob   <- Service.runJob(EtlJobArgs(name,props),"New Rest API")
          json     <- JsonApi.convertToString(etlJob,List.empty)
          response = Response.jsonString(json)
        } yield response

    }
}
