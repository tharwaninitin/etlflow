package etlflow.webserver

import etlflow.json.JsonApi
import etlflow.server.model.{EtlJob, UserArgs, UserAuth}
import etlflow.server.{ServerEnv, Service}
import zhttp.http.Method._
import zhttp.http._
import zio.UIO

object RestAPI {

  lazy val live: HttpApp[ServerEnv, Throwable] =
    Http.collectZIO { case req @ POST -> !! / "api" / "etlflow" / "runjob" / name =>
      for {
        reqStr <- req.bodyAsString
        props  <- if (reqStr != "") JsonApi.convertToObject[Map[String, String]](reqStr) else UIO(Map.empty[String, String])
        etlJob <- Service.runJob(name, props, "Rest API")
        json   <- JsonApi.convertToString(etlJob)(zio.json.DeriveJsonEncoder.gen[EtlJob])
        response = Response.json(json)
      } yield response
    }

  lazy val login: HttpApp[ServerEnv, Throwable] =
    Http.collectZIO { case req @ POST -> !! / "api" / "login" =>
      for {
        reqStr <- req.bodyAsString
        user   <- JsonApi.convertToObject[UserArgs](reqStr)(zio.json.DeriveJsonDecoder.gen[UserArgs])
        etlJob <- Service.login(user)
        json   <- JsonApi.convertToString(etlJob)(zio.json.DeriveJsonEncoder.gen[UserAuth])
        response = Response.json(json)
      } yield response
    }
}
