package etlflow.webserver

import etlflow.api.ServerTask
import etlflow.api.Service
import etlflow.utils.RequestValidator
import io.circe._
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import zio.interop.catz._

object RestAPI extends Http4sDsl[ServerTask] {

  object jobName extends QueryParamDecoderMatcher[String]("job_name")
  object props   extends OptionalQueryParamDecoderMatcher[String]("props")

  val routes: HttpRoutes[ServerTask] = HttpRoutes.of[ServerTask] {
    case GET -> Root / "runjob" :? jobName(name) +& props(props) =>
      RequestValidator(name,props) match {
        case Right(output) =>
          Service.runJob(output,"Rest API")
            .flatMap(x => Ok(Json.obj("message" -> Json.fromString(s"Job ${x.name} submitted successfully"))))
        case Left(error)   => BadRequest(error)
      }
  }
}
