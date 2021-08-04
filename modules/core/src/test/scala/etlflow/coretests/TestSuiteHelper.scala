package etlflow.coretests

import etlflow.EtlJobProps
import etlflow.db.{RunDbMigration, liveDBWithTransactor}
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.utils.{ApplicationLogger, Configuration, ReflectAPI => RF}

trait TestSuiteHelper extends ApplicationLogger {
  val config = zio.Runtime.default.unsafeRun(Configuration.config)
  val skey = config.webserver.flatMap(_.secretKey)
  val cryptoLayer = etlflow.crypto.Implementation.live(skey)
  val cacheLayer  = etlflow.cache.Implementation.live
  val canonical_path = new java.io.File(".").getCanonicalPath
  val file = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"
  val fullLayer = liveDBWithTransactor(config.db) ++ etlflow.json.Implementation.live ++ cryptoLayer ++ cacheLayer

  type MEJP = MyEtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]]
  val ejpm_package: String = RF.getJobNamePackage[MEJP] + "$"

  zio.Runtime.default.unsafeRun(RunDbMigration(config.db,clean = true))
}
