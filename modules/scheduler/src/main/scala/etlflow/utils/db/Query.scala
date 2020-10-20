package etlflow.utils.db

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.util.UUID.randomUUID

import caliban.CalibanError.ExecutionError
import cron4s.Cron
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.quill.DoobieContext
import etlflow.log.{JobRun, StepRun}
import etlflow.utils.EtlFlowHelper.Creds.{AWS, JDBC}
import etlflow.utils.EtlFlowHelper.{CronJobDB, _}
import etlflow.utils.{CacheHelper, JsonJackson}
import io.getquill.Literal
import org.slf4j.{Logger, LoggerFactory}
import pdi.jwt.{Jwt, JwtAlgorithm}
import scalacache.{Cache, Mode}
import scalacache.memoization.memoizeF
import zio.interop.catz._
import zio.{IO, Task}
import scala.collection.JavaConverters._

import scala.concurrent.duration._
object Query {

  lazy val query_logger: Logger = LoggerFactory.getLogger(getClass.getName)
  val dc = new DoobieContext.Postgres(Literal)
  import dc._

  implicit val cronJobDBCache = CacheHelper.createCache[List[CronJobDB]](24 * 60)
  implicit val mode: Mode[Task] = scalacache.CatsEffect.modes.async

  def login(args: UserArgs,transactor: HikariTransactor[Task],cache: Cache[String]): Task[UserAuth] =  {
    val q = quote {
      query[UserInfo].filter(x => x.user_name == lift(args.user_name) && x.password == lift(args.password))
    }
    dc.run(q).transact(transactor).flatMap(z => {
      if (z.isEmpty) {
        Task(UserAuth("Invalid User", ""))
      }
      else {
        Task {
          val number = randomUUID().toString.split("-")(0)
          val token = Jwt.encode(s"""${args.user_name}:$number""", "secretKey", JwtAlgorithm.HS256)
          query_logger.info("Token generated " + token)
          CacheHelper.putKey(cache,token,token)
          UserAuth("Valid User", token)
        }
      }
    })
  }

  def updateJobState(args: EtlJobStateArgs,transactor: HikariTransactor[Task]): Task[Boolean] = {
    val cronJobStringUpdate = quote {
      querySchema[CronJobDB]("cronjob")
        .filter(x => x.job_name == lift(args.name))
        .update{
          _.is_active -> lift(args.state)
        }
    }
    dc.run(cronJobStringUpdate).transact(transactor).map(_ => args.state)
  }.mapError { e =>
    query_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def getCronJobFromDB(name: String, transactor: HikariTransactor[Task]): IO[ExecutionError, CronJobDB] = {
    val cj = quote {
      querySchema[CronJobDB]("cronjob")
        .filter(x => x.job_name == lift(name))
    }
    dc.run(cj).transact(transactor).map(x => x.head)
  }.mapError { e =>
    query_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def addCredentials(args: CredentialsArgs, transactor: HikariTransactor[Task]): Task[Credentials] = {
    val credentialsDB = CredentialDB(
      args.name,
      args.`type`.get match {
        case JDBC => "jdbc"
        case AWS => "aws"
      },
      JsonJackson.convertToJsonByRemovingKeys(args.value.map(x => (x.key,x.value)).toMap, List.empty)
    )
    val credentialString = quote {
      querySchema[CredentialDB]("credentials").insert(lift(credentialsDB))
    }
    dc.run(credentialString).transact(transactor).map(_ => Credentials(credentialsDB.name,credentialsDB.`type`,credentialsDB.value))
  }.mapError { e =>
    query_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def updateCredentials(args: CredentialsArgs,transactor: HikariTransactor[Task]): Task[Credentials] = {
    val value = JsonJackson.convertToJsonByRemovingKeys(args.value.map(x => (x.key,x.value)).toMap, List.empty)
    val cronJobStringUpdate = quote {
      querySchema[CredentialDB]("credentials")
        .filter(x => x.name == lift(args.name))
        .update{
          _.value -> lift(value)
        }
    }
    dc.run(cronJobStringUpdate).transact(transactor).map(_ => Credentials(args.name,"",value))
  }.mapError { e =>
    query_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def addCronJob(args: CronJobArgs,transactor: HikariTransactor[Task]): Task[CronJob] = {
    val cronJobDB = CronJobDB(args.job_name, args.schedule.toString,0,0,is_active = true)
    val cronJobString = quote {
      querySchema[CronJobDB]("cronjob").insert(lift(cronJobDB))
    }
    dc.run(cronJobString).transact(transactor).map(_ => CronJob(cronJobDB.job_name,Cron(cronJobDB.schedule).toOption,0,0))
  }.mapError { e =>
    query_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def updateCronJob(args: CronJobArgs,transactor: HikariTransactor[Task]): Task[CronJob] = {
    val cronJobStringUpdate = quote {
      querySchema[CronJobDB]("cronjob")
        .filter(x => x.job_name == lift(args.job_name))
        .update{
          _.schedule -> lift(args.schedule.toString)
        }
    }
    dc.run(cronJobStringUpdate).transact(transactor).map(_ => CronJob(args.job_name,Some(args.schedule),0,0))
  }.mapError { e =>
    query_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def getDbStepRuns(args: DbStepRunArgs,transactor: HikariTransactor[Task]): Task[List[StepRun]] = {
    try {
      val q = quote {
        query[StepRun]
          .filter(_.job_run_id == lift(args.job_run_id))
          .sortBy(p => p.inserted_at)(Ord.desc)
      }
      dc.run(q).transact(transactor)
    }
    catch {
      case x: Throwable =>
        query_logger.error(s"Exception occurred for arguments $args")
        x.getStackTrace.foreach(msg => query_logger.error("=> " + msg.toString))
        throw x
    }
  }

  def getDbJobRuns(args: DbJobRunArgs, transactor: HikariTransactor[Task]): Task[List[JobRun]] = {

    var q: Quoted[Query[JobRun]] = null
    try {
      if (args.jobRunId.isDefined && args.jobName.isEmpty) {
        q = quote {
          query[JobRun]
            .filter(_.job_run_id == lift(args.jobRunId.get))
            .sortBy(p => p.inserted_at)(Ord.desc)
            .drop(lift(args.offset))
            .take(lift(args.limit))
        }
        // logger.info(s"Query Fragment Generated for arguments $args is ")
      }
      else if (args.jobRunId.isEmpty && args.jobName.isDefined) {
        q = quote {
          query[JobRun]
            .filter(_.job_name == lift(args.jobName.get))
            .sortBy(p => p.inserted_at)(Ord.desc)
            .drop(lift(args.offset))
            .take(lift(args.limit))
        }
        // logger.info(s"Query Fragment Generated for arguments $args is " + q.toString)
      }
      else if (args.endTime.isDefined) {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val startTime = if(args.startTime.get == "")
          sdf.parse(LocalDate.now().plusDays(1).toString).getTime
        else
          args.startTime.get.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()

        val endTime =  args.endTime.get.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
        q = quote {
          query[JobRun]
            .filter(p => p.inserted_at > lift(startTime) && p.inserted_at < lift(endTime))
            .sortBy(p => p.inserted_at)(Ord.desc)
            .drop(lift(args.offset))
            .take(lift(args.limit))
        }
        // logger.info(s"Query Fragment Generated for arguments $args is " + q.toString)
      }
      else {
        q = quote {
          query[JobRun]
            .sortBy(p => p.inserted_at)(Ord.desc)
            .drop(lift(args.offset))
            .take(lift(args.limit))
        }
        // logger.info(s"Query Fragment Generated for arguments $args is " + q.toString)
      }
      dc.run(q).transact(transactor)
    }
    catch {
      case x: Throwable =>
        // This below code should be error free always
        query_logger.error(s"Exception occurred for arguments $args")
        x.getStackTrace.foreach(msg => query_logger.error("=> " + msg.toString))
        // Throw error here or DummyResults
        throw x
    }
  }

  def getJobs(transactor: HikariTransactor[Task]): Task[List[CronJobDB]] = memoizeF[Task, List[CronJobDB]](Some(3600.second)){
    val selectQuery = quote {
      querySchema[CronJobDB]("cronjob")
    }
    dc.run(selectQuery).transact(transactor)
  }

  def getJobCacheStats:CacheInfo = {
     val data:Map[String,String] = CacheHelper.toMap(cronJobDBCache)
     CacheInfo(
       "CronJobs",
       cronJobDBCache.underlying.stats.hitCount(),
       cronJobDBCache.underlying.stats.hitRate(),
       cronJobDBCache.underlying.asMap().size(),
       cronJobDBCache.underlying.stats.missCount(),
       cronJobDBCache.underlying.stats.missRate(),
       cronJobDBCache.underlying.stats.requestCount(),
       data
     )
  }
}
