package etlflow.aws

import etlflow.model.Credential.AWS
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._
import zio._
import zio.stream._
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import zio.interop.reactivestreams._
import scala.jdk.CollectionConverters._

case class S3(client: S3AsyncClient) extends S3Api.Service {

  def createBucket(name: String): Task[CreateBucketResponse] =
    ZIO.fromCompletableFuture(client.createBucket(CreateBucketRequest.builder.bucket(name).build))

  def listBuckets: Task[List[Bucket]] = ZIO.fromCompletableFuture(client.listBuckets).map(_.buckets().asScala.toList)

  def listObjects(bucket: String, prefix: String, maxKeys: Int): Task[ListObjectsV2Response] =
    ZIO.fromCompletableFuture(
      client.listObjectsV2(
        ListObjectsV2Request.builder
          .bucket(bucket)
          .maxKeys(maxKeys)
          .prefix(prefix)
          .build
      )
    )

  def lookupObject(bucket: String, key: String): Task[Boolean] = ZIO
    .fromCompletableFuture(
      client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build())
    )
    .as(true)
    .catchSome { case _: NoSuchKeyException => UIO(false) }

  def putObject(bucket: String, key: String, file: Path, overwrite: Boolean): Task[PutObjectResponse] =
    lookupObject(bucket, key)
      .flatMap { out =>
        if (out && !overwrite) IO.fail(new IllegalArgumentException(s"File at path s3://$bucket/$key already exist"))
        else ZIO.fromCompletableFuture(client.putObject(PutObjectRequest.builder.bucket(bucket).key(key).build, file))
      }

  def putObject[R](bucket: String, key: String, content: ZStream[R, Throwable, Byte], contentLength: Long): RIO[R, Unit] =
    content
      .mapChunks(c => Chunk(ByteBuffer.wrap(c.toArray)))
      .toPublisher
      .flatMap { publisher =>
        ZIO.fromCompletableFuture(
          client.putObject(
            PutObjectRequest.builder
              .bucket(bucket)
              .key(key)
              .contentLength(contentLength)
              .build,
            AsyncRequestBody.fromPublisher(publisher)
          )
        )
      }
      .unit

  def getObject(bucket: String, key: String, file: Path): Task[GetObjectResponse] = ZIO.fromCompletableFuture(
    client.getObject(GetObjectRequest.builder.bucket(bucket).key(key).build, file)
  )

  def getObject(bucketName: String, key: String): Stream[Throwable, Byte] =
    Stream
      .fromEffect(
        ZIO.fromCompletableFuture(
          client.getObject[StreamResponse](
            GetObjectRequest.builder().bucket(bucketName).key(key).build(),
            StreamAsyncResponseTransformer(new CompletableFuture[StreamResponse]())
          )
        )
      )
      .flatMap(identity)
      .flattenChunks
      .mapError(e => S3Exception.builder().message(e.getMessage).cause(e).build())

  def delObject(bucket: String, key: String): Task[DeleteObjectResponse] = ZIO.fromCompletableFuture(
    client.deleteObject(DeleteObjectRequest.builder.bucket(bucket).key(key).build)
  )
}

object S3 {
  def live(region: Region, credentials: Option[AWS] = None, endpointOverride: Option[String] = None): TaskLayer[S3Env] =
    Managed.fromAutoCloseable(Task(S3Client(region, credentials, endpointOverride))).map(s3 => S3(s3)).toLayer
}
