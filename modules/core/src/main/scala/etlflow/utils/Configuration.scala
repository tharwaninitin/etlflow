package etlflow.utils

import etlflow.schema.Config
import zio.Runtime.default.unsafeRun
import zio.ZIO
import zio.config.magnolia.DeriveConfigDescriptor.descriptor
import zio.config.typesafe.TypesafeConfigSource
import zio.config.{ReadError, read}

private[etlflow] trait Configuration {

  private val configEN: ZIO[Any, ReadError[String], Config] =
    TypesafeConfigSource.fromDefaultLoader
      .flatMap(source => ZIO.fromEither(read(descriptor[Config] from source)))

  final lazy val config: Config = unsafeRun(configEN)
}
