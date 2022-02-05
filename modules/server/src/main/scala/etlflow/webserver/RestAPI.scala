package etlflow.webserver

import etlflow.json.JsonApi
import etlflow.server.model.{EtlJob, EtlJobArgs, Props}
import etlflow.server.{ServerEnv, Service}
import zhttp.http.Method._
import zhttp.http._

object RestAPI {
  def apply(): HttpApp[ServerEnv, Throwable] =
    Http.collectM { case req @ POST -> !! / "restapi" / "runjob" / name =>
      for {
        reqStr <- req.getBodyAsString
        props <- JsonApi
          .convertToObject[Map[String, String]](reqStr)
          .option
          .map(op => op.map(mp => mp.map(kv => Props(kv._1, kv._2)).toList))
        etlJob <- Service.runJob(EtlJobArgs(name, props), "Rest API")
        json   <- JsonApi.convertToString(etlJob)(zio.json.DeriveJsonEncoder.gen[EtlJob])
        response = Response.json(json)
      } yield response
    }
}
