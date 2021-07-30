package etlflow.webserver

import etlflow.ServerSuiteHelper
import zhttp.http.HttpData.CompleteData
import zhttp.http.URL.Location
import zhttp.http._
import zhttp.service._
import zio.test.DefaultRunnableSpec
import zio.{Chunk, Has, ZIO, ZManaged}

abstract class HttpRunnableSpec(port: Int) extends DefaultRunnableSpec with ServerSuiteHelper {

  def serve[R <: Has[_]](app: RHttpApp[R]): ZManaged[R with EventLoopGroup with ServerChannelFactory, Nothing, Unit] =
    Server.make(Server.app(app) ++ Server.port(port)).orDie

  def statusPost(path: Path,header:Option[List[Header]]): ZIO[EventLoopGroup with ChannelFactory, Throwable, Status] =
    requestPathPost(path,header.getOrElse(List.empty)).map(_.status)

  def statusGet(path: Path): ZIO[EventLoopGroup with ChannelFactory, Throwable, Status] =
    requestPathGet(path).map(_.status)

  def requestPathPost(path: Path,header:List[Header]): ZIO[EventLoopGroup with ChannelFactory, Throwable, UHttpResponse] =
    Client.request(Method.POST -> URL(path, Location.Absolute(Scheme.HTTP, "localhost", port)), header)

  def requestPathGet(path: Path): ZIO[EventLoopGroup with ChannelFactory, Throwable, UHttpResponse] =
    Client.request(Method.GET -> URL(path, Location.Absolute(Scheme.HTTP, "localhost", port)))

  def request(path: Path, method: Method, content: String): ZIO[EventLoopGroup with ChannelFactory, Throwable, UHttpResponse] = {
    val data = CompleteData(Chunk.fromArray(content.getBytes(HTTP_CHARSET)))
    Client.request(Request(method -> URL(path, Location.Absolute(Scheme.HTTP, "localhost", port)),List.empty, data))
  }
}
