package etlflow.server.model

case class EtlFlowMetrics(
    active_jobs: Int,
    active_subscribers: Int,
    etl_jobs: Int,
    cron_jobs: Int,
    build_time: String
)
