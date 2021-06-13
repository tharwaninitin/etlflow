//package etlflow
//
//import etlflow.jdbc.{DBEnv, DBServerEnv, liveDBWithTransactor}
//import etlflow.utils.DbManager
//import zio.blocking.Blocking
//import zio.{Chunk, Fiber, Runtime, Semaphore, Supervisor, ZLayer}
//
//trait ServerSuiteHelper extends DbManager  {
//
//  type MEJP = MyEtlJobPropsMapping[EtlJobProps, CoreEtlJob[EtlJobProps]]
//  val config: Config = io.circe.config.parser.decode[Config]().toOption.get
//  val credentials: JDBC = config.dbLog
//  val ejpm_package: String = UF.getJobNamePackage[MEJP] + "$"
//  val sem: Map[String, Semaphore] = Map("Job1" -> Runtime.default.unsafeRun(Semaphore.make(1)))
//  val supervisor: Supervisor[Chunk[Fiber.Runtime[Any, Any]]] = Runtime.default.unsafeRun(Supervisor.track(true))
//  val testDBLayer: ZLayer[Blocking, Throwable, DBServerEnv with DBEnv] = liveDBWithTransactor(config.dbLog)
//
//}
