package etlflow

import org.slf4j.{Logger, LoggerFactory}
import zio.{Has, ZIO}

package object local {
  val gcp_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  type  LocalService = Has[LocalService.Service]

  object LocalService {
    trait Service {
      def executeLocalJob(name: String, properties: Map[String,String]): ZIO[LocalService, Throwable, Unit]

    }
    def executeLocalJob(name: String, properties: Map[String,String]): ZIO[LocalService, Throwable, Unit] =
      ZIO.accessM(_.get.executeLocalJob(name, properties))
  }
}
