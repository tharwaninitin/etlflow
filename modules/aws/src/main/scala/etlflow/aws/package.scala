package etlflow

import software.amazon.awssdk.core.async.{AsyncResponseTransformer, SdkPublisher}
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import zio.interop.reactivestreams._
import zio.stream.ZStream
import zio.{Chunk, Has}
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

package object aws {
  type StreamResponse = ZStream[Any, Throwable, Chunk[Byte]]
  type S3Env          = Has[S3Api.Service]

  final private[aws] case class StreamAsyncResponseTransformer(cf: CompletableFuture[StreamResponse])
      extends AsyncResponseTransformer[GetObjectResponse, StreamResponse] {
    override def prepare(): CompletableFuture[StreamResponse] = cf

    override def onResponse(response: GetObjectResponse): Unit = ()

    override def onStream(publisher: SdkPublisher[ByteBuffer]): Unit = {
      cf.complete(publisher.toStream().map(Chunk.fromByteBuffer))
      ()
    }

    override def exceptionOccurred(error: Throwable): Unit = {
      cf.completeExceptionally(error)
      ()
    }
  }
}
