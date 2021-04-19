package etlflow

import zio.ZIO

package object executor {
  trait Service {
    def executeJob(name: String, properties: Map[String,String]): ZIO[JobEnv, Throwable, Unit]
  }
}
