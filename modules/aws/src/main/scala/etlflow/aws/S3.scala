package etlflow.aws

import etlflow.model.Credential.AWS
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._
import zio._
import zio.stream.Stream
import java.nio.file.Paths
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

case class S3(client: S3AsyncClient) extends S3Api.Service {

  def listBuckets: Task[ListBucketsResponse] = ZIO.fromCompletableFuture(client.listBuckets)

  def listBucketObjects(bucket: String, prefix: String, maxKeys: Int): Task[ListObjectsV2Response] =
    ZIO.fromCompletableFuture(
      client.listObjectsV2(
        ListObjectsV2Request.builder
          .bucket(bucket)
          .maxKeys(maxKeys)
          .prefix(prefix)
          .build
      )
    )

  def lookupObject(bucket: String, prefix: String, key: String): Task[Boolean] = for {
    list <- listBucketObjects(bucket, prefix, Integer.MAX_VALUE)
    _ = logger.info {
      if (list.contents().asScala.nonEmpty)
        s"Objects in bucket $bucket with prefix $prefix are \n" + list.contents().asScala.mkString("\n")
      else s"No objects in bucket $bucket with prefix $prefix"
    }
    newKey = prefix + "/" + key
    res    = list.contents.asScala.exists(_.key == newKey)
  } yield res

  def putObject(bucket: String, key: String, file: String): Task[PutObjectResponse] = ZIO.fromCompletableFuture(
    client.putObject(PutObjectRequest.builder.bucket(bucket).key(key).build, Paths.get(file))
  )

  def getObject(bucket: String, key: String, file: String): Task[GetObjectResponse] = ZIO.fromCompletableFuture(
    client.getObject(GetObjectRequest.builder.bucket(bucket).key(key).build, Paths.get(file))
  )

  def getObject(bucketName: String, key: String): Stream[S3Exception, Byte] =
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
      .refineOrDie { case e: S3Exception => e }

  def delObject(bucket: String, key: String): Task[DeleteObjectResponse] = ZIO.fromCompletableFuture(
    client.deleteObject(DeleteObjectRequest.builder.bucket(bucket).key(key).build)
  )
}

object S3 {
  def live(region: Region, credentials: Option[AWS] = None, endpointOverride: Option[String] = None): TaskLayer[S3Env] =
    Managed.fromAutoCloseable(Task(S3Client(region, credentials, endpointOverride))).map(s3 => S3(s3)).toLayer
}
