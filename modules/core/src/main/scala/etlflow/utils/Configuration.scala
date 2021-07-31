package etlflow.utils

import etlflow.schema.Config
import zio.{ZIO, IO}
import zio.config.magnolia.DeriveConfigDescriptor.descriptor
import zio.config.typesafe.TypesafeConfigSource
import zio.config.{ReadError, read}

object Configuration {
  lazy val config: IO[ReadError[String], Config] =
    TypesafeConfigSource.fromDefaultLoader
      .flatMap(source => ZIO.fromEither(read(descriptor[Config] from source)))
}
