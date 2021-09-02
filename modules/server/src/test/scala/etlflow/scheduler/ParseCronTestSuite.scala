package etlflow.scheduler

import zio.{Task, ZIO}
import zio.test.Assertion.equalTo
import zio.test._

object ParseCronTestSuite extends Scheduler {

  val spec: ZSpec[environment.TestEnvironment, Any] = {
    suite("Cron Parser")(
      testM("parseCron Should parse cron correctly: A run frequency of once at 16:25 on December 18, 2018 ") {
        assertM(Task.fromTry(parseCron("0 25 16 18 DEC ? 2018")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("parseCron Should parse cron correctly: A run frequency of 12:00 PM (noon) every day") {
        assertM(Task.fromTry(parseCron("0 0 12 * * ?")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("parseCron Should parse cron correctly: A run frequency of 11:00 PM every weekday night") {
        assertM(Task.fromTry(parseCron("0 0 23 ? * MON-FRI")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("parseCron Should parse cron correctly: A run frequency of 10:15 AM every day") {
        assertM(Task.fromTry(parseCron("0 15 10 * * ?")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("parseCron Should parse cron correctly: A run frequency of 10:15 AM every Monday, Tuesday, Wednesday, Thursday and Friday") {
        assertM(Task.fromTry(parseCron("0 15 10 ? * MON-FRI")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("parseCron Should parse cron correctly: A run frequency of 12:00 PM (noon) every first day of the month") {
        assertM(Task.fromTry(parseCron("0 0 12 1 1/1 ? *")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("parseCron Should parse cron correctly: A run frequency of every hour between 8:00 AM and 5:00 PM Monday-Friday") {
        assertM(Task.fromTry(parseCron("0 0 8-17 ? * MON-FRI")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("parseCron Should return Error when incorrect cron  provided") {
        assertM(Task.fromTry(parseCron("0 */2 * ")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("Cron expression contains 3 parts but we expect one of [6, 7]"))
      },
      testM("parseCron Should return Error when incorrect cron  provided") {
        assertM(Task.fromTry(parseCron("0 0 8-17 ? * MON-FRII")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("Failed to parse '0 0 8-17 ? * MON-FRII'. Invalid chars in expression! Expression: FRII Invalid chars: FRII"))
      },
      testM("parseCron Should return Error when incorrect cron  provided: Seconds are out of range") {
        assertM(Task.fromTry(parseCron("155 15 10 ? * MON-FRI")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("Failed to parse '155 15 10 ? * MON-FRI'. Value 155 not in range [0, 59]"))
      },
      testM("parseCron Should return Error when incorrect cron  provided: Minutes are out of range") {
        assertM(Task.fromTry(parseCron("0 155 10 ? * MON-FRI")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("Failed to parse '0 155 10 ? * MON-FRI'. Value 155 not in range [0, 59]"))
      },
      testM("parseCron Should return Error when incorrect cron  provided: Hours are out of range") {
        assertM(Task.fromTry(parseCron("0 15 100 ? * MON-FRI")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("Failed to parse '0 15 100 ? * MON-FRI'. Value 100 not in range [0, 23]"))
      },
      testM("parseCron Should return Error when incorrect cron  provided: DayOfMonth are out of range") {
        assertM(Task.fromTry(parseCron("0 15 10 33 * MON-FRI")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("Failed to parse '0 15 10 33 * MON-FRI'. Value 33 not in range [1, 31]"))
      },
      testM("parseCron Should return Error when incorrect cron  provided: Months are out of range") {
        assertM(Task.fromTry(parseCron("0 15 10 ? 13 MON-FRI")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("Failed to parse '0 15 10 ? 13 MON-FRI'. Value 13 not in range [1, 12]"))
      },
    )
  }
}
