package etlflow

import zio.ZIO

package object executor {
  private[etlflow] trait Service {
    def executeJob(name: String, properties: Map[String,String]): ZIO[JobEnv, Throwable, Unit]
  }
}
