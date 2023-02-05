package etlflow

import sttp.client3.Response
import zio.{Task, ULayer, ZIO, ZLayer}

package object http {

  val noop: ULayer[Http] = ZLayer.succeed(
    new Http {
      @SuppressWarnings(Array("org.wartremover.warts.ToString"))
      override def execute(
          method: HttpMethod,
          url: String,
          params: Either[String, Map[String, String]],
          headers: Map[String, String],
          logDetails: Boolean,
          connectionTimeout: Long,
          readTimeout: Long,
          allowUnsafeSsl: Boolean
      ): Task[Response[String]] = ZIO.logInfo(s"Sent ${method.toString} request to $url") *>
        ZIO.attempt(Response.ok[String]("noop"))
    }
  )
}
