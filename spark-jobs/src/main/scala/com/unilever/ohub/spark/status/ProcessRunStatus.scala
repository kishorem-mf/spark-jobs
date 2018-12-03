package com.unilever.ohub.spark.status

import java.sql.Timestamp

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import scalikejdbc._

case class ProcessRunStatusConfig(
    runId: String = "run-id",
    command: String = "start-or-stop",
    dbUrl: String = "db-url",
    dbTable: String = "db-table",
    dbUsername: String = "db-username",
    dbPassword: String = "db-password"
) extends SparkJobConfig

case class Run(run_id: Long, inbound_status: Option[String], inbound_start: Option[Timestamp], inbound_stop: Option[Timestamp],
    engine_status: Option[String], engine_start: Option[Timestamp], engine_stop: Option[Timestamp],
    outbound_status: Option[String], outbound_start: Option[Timestamp], outbound_stop: Option[Timestamp])

object ProcessRunStatus extends SparkJob[ProcessRunStatusConfig] {

  override private[spark] def defaultConfig = ProcessRunStatusConfig()

  override private[spark] def configParser(): OptionParser[ProcessRunStatusConfig] =
    new scopt.OptionParser[ProcessRunStatusConfig]("Spark job default") {
      head("run a spark job with default config.", "1.0")

      opt[String]("runId") required () action { (x, c) ⇒
        c.copy(runId = x)
      } text "runId is a string property"
      opt[String]("command") required () action { (x, c) ⇒
        c.copy(command = x)
      } text "command is a string property"
      opt[String]("dbUrl") required () action { (x, c) ⇒
        c.copy(dbUrl = x)
      } text "dbUrl is a string property"
      opt[String]("dbTable") required () action { (x, c) ⇒
        c.copy(dbTable = x)
      } text "dbTable is a string property"
      opt[String]("dbUsername") required () action { (x, c) ⇒
        c.copy(dbUsername = x)
      } text "dbUsername is a string property"
      opt[String]("dbPassword") required () action { (x, c) ⇒
        c.copy(dbPassword = x)
      } text "dbPassword is a string property"

      version("1.0")
      help("help") text "help text"
    }

  def updateRunStatus(config: ProcessRunStatusConfig, ctxPoolName: Any)(implicit session: DBSession): Unit = {

    object Run extends SQLSyntaxSupport[Run] {

      override val connectionPoolName: Any = ctxPoolName

      override val tableName: String = config.dbTable

      def apply(p: SyntaxProvider[Run])(rs: WrappedResultSet): Run = apply(p.resultName)(rs)
      def apply(p: ResultName[Run])(rs: WrappedResultSet): Run =
        new Run(
          run_id = rs.long(p.run_id),
          inbound_status = rs.stringOpt(p.inbound_status),
          inbound_start = rs.timestampOpt(p.inbound_start),
          inbound_stop = rs.timestampOpt(p.inbound_stop),
          engine_status = rs.stringOpt(p.engine_status),
          engine_start = rs.timestampOpt(p.engine_start),
          engine_stop = rs.timestampOpt(p.engine_stop),
          outbound_status = rs.stringOpt(p.outbound_status),
          outbound_start = rs.timestampOpt(p.outbound_start),
          outbound_stop = rs.timestampOpt(p.outbound_stop)
        )
    }

    val r = Run.syntax("r")

    val run = withSQL {
      select
        .from(Run as r)
        .where
        .eq(r.run_id, config.runId.toLong)
    }.map(Run(r)).single.apply

    run match {
      case Some(x) if x.inbound_status.contains("completed") ⇒
        if ("start" == config.command) {
          log.info(s"About to set the run status to 'started' for run with id '${config.runId}'.")

          withSQL {
            update(Run)
              .set(
                Run.column.engine_status -> "started",
                Run.column.engine_start -> new Timestamp(System.currentTimeMillis())
              )
              .where.eq(Run.column.run_id, config.runId.toLong)
          }.update().apply
        } else if ("stop" == config.command) {
          log.info(s"About to set the run status to 'completed' for run with id '${config.runId}'.")

          withSQL {
            update(Run)
              .set(
                Run.column.engine_status -> "completed",
                Run.column.engine_stop -> new Timestamp(System.currentTimeMillis())
              )
              .where.eq(Run.column.run_id, config.runId.toLong)
          }.update().apply
        } else {
          throw new IllegalArgumentException(s"invalid command '${config.command}' given for run with id '${config.runId}'.")
        }
      case Some(_) ⇒
        throw new IllegalArgumentException(s"inbound is not completed yet for run with id '${config.runId}'.")
      case None ⇒
        log.error(s"NO RUN FOUND FOR RUN ID '${config.runId}'")
        throw new IllegalArgumentException(s"NO RUN FOUND FOR RUN ID '${config.runId}'")
    }
  }

  override def run(spark: SparkSession, config: ProcessRunStatusConfig, storage: Storage): Unit = {
    ConnectionPool.singleton(config.dbUrl, config.dbUsername, config.dbPassword)

    implicit val session: AutoSession.type = AutoSession

    updateRunStatus(config, ConnectionPool.DEFAULT_NAME)
  }
}
