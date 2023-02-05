package etlflow.http

import sttp.client3.Response
import zio.{RIO, Task, TaskLayer, ZIO, ZLayer}

trait Http {

  /** @param method
    * @param url
    * @param params
    * @param headers
    * @param logDetails
    * @param connectionTimeout
    * @param readTimeout
    * @param allowUnsafeSsl
    * @return
    */
  def execute(
      method: HttpMethod,
      url: String,
      params: Either[String, Map[String, String]],
      headers: Map[String, String],
      logDetails: Boolean,
      connectionTimeout: Long,
      readTimeout: Long,
      allowUnsafeSsl: Boolean = false
  ): Task[Response[String]]
}
object Http {

  /** @param method
    * @param url
    * @param params
    * @param headers
    * @param logDetails
    * @param connectionTimeout
    * @param readTimeout
    * @param allowUnsafeSsl
    * @return
    */
  def execute(
      method: HttpMethod,
      url: String,
      params: Either[String, Map[String, String]],
      headers: Map[String, String],
      logDetails: Boolean,
      connectionTimeout: Long,
      readTimeout: Long,
      allowUnsafeSsl: Boolean = false
  ): RIO[Http, Response[String]] = ZIO.environmentWithZIO[Http](
    _.get.execute(method, url, params, headers, logDetails, connectionTimeout, readTimeout, allowUnsafeSsl)
  )

  val live: TaskLayer[Http] = ZLayer.succeed(HttpImpl)
}
