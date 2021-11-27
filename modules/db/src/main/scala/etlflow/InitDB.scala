package etlflow

import etlflow.db.RunDbMigration
import etlflow.utils.Configuration
import zio.{ExitCode, URIO}

object InitDB extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    Configuration.config.flatMap(cfg => RunDbMigration(cfg.db.get)).exitCode
}
