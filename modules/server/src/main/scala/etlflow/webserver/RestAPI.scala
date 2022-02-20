package etlflow.webserver

import etlflow.json.JsonApi
import etlflow.server.model.EtlJob
import etlflow.server.{ServerEnv, Service}
import zhttp.http.Method._
import zhttp.http._

object RestAPI {
  lazy val live: HttpApp[ServerEnv, Throwable] =
    Http.collectZIO { case req @ POST -> !! / "api" / "etlflow" / "runjob" / name =>
      for {
        reqStr <- req.bodyAsString
        props  <- JsonApi.convertToObject[Map[String, String]](reqStr)
        etlJob <- Service.runJob(name, props, "Rest API")
        json   <- JsonApi.convertToString(etlJob)(zio.json.DeriveJsonEncoder.gen[EtlJob])
        response = Response.json(json)
      } yield response
    }
}
