package com.unilever.ohub.spark.status

import java.sql.Timestamp
import java.time.LocalDateTime

import org.scalatest.{ BeforeAndAfter, Matchers, fixture }
import scalikejdbc.config.DBsWithEnv
import scalikejdbc._
import scalikejdbc.scalatest.AutoRollback

class ProcessRunStatusSpec extends fixture.FlatSpec with Matchers with AutoRollback with BeforeAndAfter {

  val SUT: ProcessRunStatus.type = ProcessRunStatus

  before {
    DBsWithEnv("test").setup('h2testdb)
  }

  after {
    DBsWithEnv("test").close('h2testdb)
  }

  override def db: DB = NamedDB('h2testdb).toDB()

  override def fixture(implicit session: DBSession): Unit = {
    sql"""
         CREATE TABLE IF NOT EXISTS runs (
            run_id serial not null primary key,
            inbound_status varchar(64),
            inbound_start timestamp,
            inbound_stop timestamp,
            engine_status varchar(64),
            engine_start timestamp,
            engine_stop timestamp,
            outbound_status varchar(64),
            outbound_start timestamp,
            outbound_stop timestamp
         )
       """.execute.apply

    val timestamp = Timestamp.valueOf(LocalDateTime.now)

    sql"insert into runs values (1, 'completed', $timestamp, $timestamp, null, null, null, null, null, null)".update.apply
    sql"insert into runs values (2, null, null, null, null, null, null, null, null, null)".update.apply
  }

  object Run extends SQLSyntaxSupport[Run] {
    override val tableName = "runs"

    def apply(rs: WrappedResultSet): Run =
      new Run(
        run_id = rs.long("run_id"),
        inbound_status = rs.stringOpt("inbound_status"),
        inbound_start = rs.timestampOpt("inbound_start"),
        inbound_stop = rs.timestampOpt("inbound_stop"),
        engine_status = rs.stringOpt("engine_status"),
        engine_start = rs.timestampOpt("engine_start"),
        engine_stop = rs.timestampOpt("engine_stop"),
        outbound_status = rs.stringOpt("outbound_status"),
        outbound_start = rs.timestampOpt("outbound_start"),
        outbound_stop = rs.timestampOpt("outbound_stop")
      )
  }

  behavior of "ProcessRunStatus"

  it should "mark a run started" in { implicit session ⇒
    val config = testConfig(runId = "1", command = "start")

    SUT.updateRunStatus(config, 'h2testdb)

    val result = sql"select * from runs where run_id = '1'".map(rs ⇒ Run(rs)).single().apply()

    result shouldBe defined
    result.head.engine_status shouldBe Some("started")
    result.head.engine_start shouldBe defined
  }

  it should "mark a run completed" in { implicit session ⇒
    val config = testConfig(runId = "1", command = "stop")

    SUT.updateRunStatus(config, 'h2testdb)

    val result = sql"select * from runs where run_id = '1'".map(rs ⇒ Run(rs)).single().apply()

    result shouldBe defined
    result.head.engine_status shouldBe Some("completed")
    result.head.engine_stop shouldBe defined
  }

  it should "fail when an invalid command is given" in { implicit session ⇒
    val config = testConfig(runId = "1", command = "invalid")

    intercept[IllegalArgumentException] {
      SUT.updateRunStatus(config, 'h2testdb)
    }.getMessage shouldBe "invalid command 'invalid' given for run with id '1'."
  }

  it should "fail when inbound has not completed yet" in { implicit session ⇒
    val config = testConfig(runId = "2", command = "stop")

    intercept[IllegalArgumentException] {
      SUT.updateRunStatus(config, 'h2testdb)
    }.getMessage shouldBe "inbound is not completed yet for run with id '2'."
  }

  it should "fail when an invalid run id is provided" in { implicit session ⇒
    val config = testConfig(runId = "invalid-run-id", command = "stop")

    intercept[java.lang.NumberFormatException] {
      SUT.updateRunStatus(config, 'h2testdb)
    }
  }

  it should "fail when the run could not be found" in { implicit session ⇒
    val config = testConfig(runId = "-1", command = "stop")

    intercept[IllegalArgumentException] {
      SUT.updateRunStatus(config, 'h2testdb)
    }
  }

  private def testConfig(runId: String, command: String) =
    ProcessRunStatusConfig(
      runId = runId,
      command = command,
      dbUrl = "jdbc:h2:file:./target/h2/test/mark_completed",
      dbTable = "runs",
      dbUsername = "user",
      dbPassword = "pass"
    )
}
