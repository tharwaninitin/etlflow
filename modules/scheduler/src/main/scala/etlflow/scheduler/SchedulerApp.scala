package etlflow.scheduler

import caliban.CalibanError.ExecutionError
import caliban.{CalibanError, GraphQLInterpreter}
import doobie.hikari.HikariTransactor
import etlflow.etljobs.{EtlJob => EtlFlowEtlJob}
import etlflow.scheduler.EtlFlowHelper.{EtlFlow, EtlFlowHas, EtlFlowInfo, EtlJob, EtlJobArgs, EtlJobNameArgs, EtlJobStatus}
import etlflow.{EtlJobName, EtlJobProps}
import etlflow.utils.{GlobalProperties, UtilityFunctions => UF}
import org.slf4j.{Logger, LoggerFactory}
import zio.{Queue, Ref, Task, UIO, ZEnv, ZIO, ZLayer}
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.{Logger => LBLogger}
import zio.stream.ZStream
import scala.reflect.runtime.universe.TypeTag

abstract class SchedulerApp[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag, EJGP <: GlobalProperties : TypeTag] extends GQLServerHttp4s {
  lazy val logger: Logger        = LoggerFactory.getLogger(getClass.getName)
  lazy val root_logger: LBLogger = LoggerFactory.getLogger("org.apache.spark").asInstanceOf[LBLogger]
  root_logger.setLevel(Level.WARN)

  def globalProperties: Option[EJGP]
  val etl_job_name_package: String
  def toEtlJob(job_name: EJN): (EJP,Option[EJGP]) => EtlFlowEtlJob

  private lazy val global_properties: Option[EJGP] = globalProperties
  val DB_DRIVER: String = "org.postgresql.Driver"
  val DB_URL: String  = global_properties.map(x => x.log_db_url).getOrElse("<not_set>")     // connect URL
  val DB_USER: String = global_properties.map(x => x.log_db_user).getOrElse("<not_set>")    // username
  val DB_PASS: String = global_properties.map(x => x.log_db_pwd).getOrElse("<not_set>")    // password

  object EtlFlowService {
    def liveHttp4s(pgTransactor: HikariTransactor[Task]): ZLayer[Any, Throwable, EtlFlowHas] =
      ZLayer.fromEffect{
        for {
          subscribers <- Ref.make(List.empty[Queue[EtlJobStatus]])
          activeJobs  <- Ref.make(0)
        } yield EtlFlowService(pgTransactor,activeJobs,subscribers)
      }
  }

  final case class EtlFlowService(transactor: HikariTransactor[Task], activeJobs: Ref[Int], subscribers: Ref[List[Queue[EtlJobStatus]]]) extends EtlFlow.Service {
    val logger: Logger = LoggerFactory.getLogger(getClass.getName)

    override def getEtlJobs(args: EtlJobNameArgs): ZIO[EtlFlowHas, Throwable, List[EtlJob]] = {
      Task{
        UF.getEtlJobs[EJN].map(x => EtlJob(x,getJobProps(x))).toList
      }.mapError{ e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
    }

    override def runJob(args: EtlJobArgs): ZIO[EtlFlowHas, Throwable, EtlJob] = {

      val etlJobDetails: Task[(EJN, EtlFlowEtlJob, Map[String, String])] = Task {
        val job_name      = UF.getEtlJobName[EJN](args.name, etl_job_name_package)
        val props_map     = args.props.map(x => (x.key,x.value)).toMap
        val etl_job       = toEtlJob(job_name)(job_name.getActualProperties(props_map),globalProperties)
        etl_job.job_name  = job_name.toString
        (job_name,etl_job,props_map)
      }.mapError(e => ExecutionError(e.getMessage))

      val job = for {
        queues                       <- subscribers.get
        (job_name,etl_job,props_map) <- etlJobDetails
        execution_props <- Task {
                              UF.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
                                .map(x => (x._1, x._2.toString))
                            }.mapError{ e =>
                              logger.error(e.getMessage)
                              ExecutionError(e.getMessage)
                            }
        _               <- activeJobs.update(_ + 1)
        _               <- UIO.foreach(queues){queue =>
                             queue
                              .offer(EtlJobStatus(etl_job.job_name,"Started",execution_props))
                              .catchSomeCause {
                                case cause if cause.interrupted =>
                                  subscribers.update(_.filterNot(_ == queue)).as(false)
                              } // if queue was shutdown, remove from subscribers
                           }
        _               <- etl_job.execute().ensuring{
                            activeJobs.update(_ - 1) *>
                            UIO.foreach(queues){queue =>
                              queue
                                .offer(EtlJobStatus(etl_job.job_name,"Completed",execution_props))
                                .catchSomeCause {
                                  case cause if cause.interrupted =>
                                    subscribers.update(_.filterNot(_ == queue)).as(false)
                                } // if queue was shutdown, remove from subscribers
                            }
                           }.forkDaemon
      } yield EtlJob(args.name,execution_props)
      job
    }

    override def getInfo: ZIO[EtlFlowHas, Throwable, EtlFlowInfo] = {
      for {
        x <- activeJobs.get
        y <- subscribers.get
      } yield EtlFlowInfo(x, y.length)
    }

    override def notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus] = ZStream.unwrap {
      for {
        queue <- Queue.unbounded[EtlJobStatus]
        _     <- subscribers.update(queue :: _)
      } yield ZStream.fromQueue(queue).ensuring(queue.shutdown)
    }

    def getStream: ZStream[EtlFlowHas, Throwable, EtlFlowInfo] = {
      ZStream.fromIterable(Seq(EtlFlowInfo(1,1), EtlFlowInfo(2,2), EtlFlowInfo(3,3)))
    }

    def getLogs: ZIO[EtlFlowHas, Throwable, EtlFlowInfo] = {
      // ZStream.fromIterable(Seq(EtlFlowInfo(1), EtlFlowInfo(2), EtlFlowInfo(3))).runCollect.map(x => x.head)
      ZStream.fromIterable(Seq(EtlFlowInfo(1,1), EtlFlowInfo(2,2), EtlFlowInfo(3,3))).runCollect.map(x => x.head)
      // Command("echo", "-n", "1\n2\n3").linesStream.runCollect.map(x => EtlFlowInfo(x.head.length))
    }
  }

  def etlFlowHttp4sInterpreter(transactor: HikariTransactor[Task]): ZIO[Any, CalibanError, GraphQLInterpreter[ZEnv, Throwable]] =
    EtlFlowService.liveHttp4s(transactor)
      .memoize
      .use {
        layer => EtlFlowApi.api.interpreter.map(_.provideCustomLayer(layer))
      }

  private def getJobProps(job_name: String): Map[String, String] = {
    val name = UF.getEtlJobName[EJN](job_name,etl_job_name_package)
    val exclude_keys = List("job_run_id","job_description","job_properties")
    UF.convertToJsonByRemovingKeysAsMap(name.default_properties,exclude_keys).map(x =>(x._1,x._2.toString))
  }
}
