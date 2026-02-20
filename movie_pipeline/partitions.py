from dagster import TimeWindowPartitionsDefinition


yearly_partitions = TimeWindowPartitionsDefinition(
    start="1995-01-01",
    end="2024-01-01",
    cron_schedule="@yearly",
    fmt="%Y",
)
