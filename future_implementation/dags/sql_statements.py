

COPY_SQL = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{{}}'
    SECRET_ACCESS_KEY '{{}}'
    IGNOREHEADER 1
    DELIMITER ','
"""

COPY_MONTHLY_TRIPS_SQL = COPY_SQL.format(
    "trips",
    "s3://udacity-dend/data-pipelines/divvy/partitioned/{year}/{month}/divvy_trips.csv"
)
