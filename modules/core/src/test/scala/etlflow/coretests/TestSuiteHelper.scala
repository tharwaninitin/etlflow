package etlflow.coretests

import etlflow.EtlJobProps
import etlflow.db.{RunDbMigration, liveDB}
import etlflow.etljobs.EtlJob
import etlflow.utils.{ApplicationLogger, Configuration}

trait TestSuiteHelper extends ApplicationLogger {
  val config = zio.Runtime.default.unsafeRun(Configuration.config)
  val skey = config.webserver.flatMap(_.secretKey)
  val cryptoLayer = etlflow.crypto.Implementation.live(skey)
  val jsonLayer  = etlflow.json.Implementation.live
  val loggerLayer = etlflow.log.Implementation.live
  val dbLayer = etlflow.db.liveDB(config.db)
  val slackNoLoggerLayer = etlflow.log.SlackApi.nolog
  val fullLayer = dbLayer ++ jsonLayer ++ cryptoLayer ++ loggerLayer ++ slackNoLoggerLayer

  val canonical_path = new java.io.File(".").getCanonicalPath
  val file = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"

  type MEJP = MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]

  zio.Runtime.default.unsafeRun(RunDbMigration(config.db,clean = true))
}
