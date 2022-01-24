package etlflow.webserver

import etlflow.server.APIEnv
import etlflow.db.DBServerEnv
import etlflow.json.JsonEnv
import etlflow.log.LogEnv
import zhttp.http.URL.Location
import zhttp.http._
import zhttp.service._
import zhttp.service.client.ClientSSLHandler.ClientSSLOptions.DefaultSSL
import zio.{Chunk, Has, ZIO, ZManaged}

abstract class HttpRunnableSpec(port: Int) {

  type TestAuthEnv = EventLoopGroup
    with ChannelFactory
    with zhttp.service.ServerChannelFactory
    with APIEnv
    with DBServerEnv
    with LogEnv
    with JsonEnv

  def url(path: Path) = URL(path, Location.Absolute(Scheme.HTTP, "localhost", port))

  def serve[R <: Has[_]](app: RHttpApp[R]): ZManaged[R with EventLoopGroup with ServerChannelFactory, Nothing, Unit] =
    Server.make(Server.app(app) ++ Server.port(port)).orDie

  def statusPost(
      path: Path,
      headers: Headers = Headers.empty
  ): ZIO[EventLoopGroup with ChannelFactory, Throwable, Status] =
    requestPathPost(path, headers).map(_.status)

  def statusGet(path: Path): ZIO[EventLoopGroup with ChannelFactory, Throwable, Status] =
    requestPathGet(path).map(_.status)

  def requestPathPost(
      path: Path,
      headers: Headers
  ): ZIO[EventLoopGroup with ChannelFactory, Throwable, Client.ClientResponse] =
    Client.request(Method.POST, url(path), headers, sslOptions = DefaultSSL)

  def requestPathGet(path: Path): ZIO[EventLoopGroup with ChannelFactory, Throwable, Client.ClientResponse] =
    Client.request(Method.GET, url(path))

  def request(
      path: Path,
      method: Method,
      content: String
  ): ZIO[EventLoopGroup with ChannelFactory, Throwable, Client.ClientResponse] = {
    val data = HttpData.fromChunk(Chunk.fromArray(content.getBytes(HTTP_CHARSET)))
    Client.request(method, url(path), Headers.empty, data)
  }
}
