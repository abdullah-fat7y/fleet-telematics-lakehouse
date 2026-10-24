# Fabric notebook 06: prove bronze_telemetry is append-only, then enforce it.
#
# DESCRIBE HISTORY cannot prove it here: the Eventstream writer leaves `operation` empty. So this
# notebook reads the Delta transaction log itself (_delta_log/*.json), which records every file
# added or removed by every commit, whoever wrote it:
#   Check 1. No commit ever removed data. A `remove` with dataChange = true would be a delete,
#            update or overwrite. Compaction (OPTIMIZE) removes files with dataChange = false and
#            re-adds the same rows - that is allowed and shown separately.
#   Check 2. Every row ever written is still there: rows added by data commits = rows in the table now.
# Then it sets delta.appendOnly = true, so Delta itself refuses deletes and updates from now on,
# and shows a DELETE being refused (the DELETE matches no rows, so nothing could be lost either way).

from pyspark.sql import functions as F

TABLE = "bronze_telemetry"

detail = spark.sql(f"DESCRIBE DETAIL {TABLE}").first()
log = (spark.read.json(f"{detail['location']}/_delta_log/*.json")
       .withColumn("version", F.regexp_extract(F.col("_metadata.file_path"), r"(\d+)\.json$", 1).cast("long")))
print(f"{TABLE}: {detail['numFiles']} files now, {log.select('version').distinct().count()} commits in the log")


def actions(kind):
    """add / remove actions per commit. A missing dataChange counts as a data change (strictest reading)."""
    if kind not in log.columns:
        return spark.createDataFrame([], "version long, data_change boolean, rows long")
    return log.filter(F.col(kind).isNotNull()).select(
        "version",
        F.coalesce(F.col(f"{kind}.dataChange"), F.lit(True)).alias("data_change"),
        (F.get_json_object(f"{kind}.stats", "$.numRecords").cast("long")
         if "stats" in log.schema[kind].dataType.fieldNames() else F.lit(None).cast("long")).alias("rows"))


adds, removes = actions("add"), actions("remove")
commit_info = (log.filter(F.col("commitInfo").isNotNull())
               .select("version", F.col("commitInfo.operation").alias("operation"),
                       F.from_unixtime(F.col("commitInfo.timestamp") / 1000).alias("committed_at"))
               if "commitInfo" in log.columns else spark.createDataFrame([], "version long, operation string, committed_at string"))

per_commit = (log.select("version").distinct()
              .join(commit_info, "version", "left")
              .join(adds.groupBy("version").agg(
                  F.sum(F.when(F.col("data_change"), 1).otherwise(0)).alias("files_added"),
                  F.sum(F.when(F.col("data_change"), F.col("rows"))).alias("rows_added"),
                  F.sum(F.when(~F.col("data_change"), 1).otherwise(0)).alias("files_rewritten_by_compaction")),
                  "version", "left")
              .join(removes.groupBy("version").agg(
                  F.sum(F.when(F.col("data_change"), 1).otherwise(0)).alias("files_removed_with_data"),
                  F.sum(F.when(~F.col("data_change"), 1).otherwise(0)).alias("files_compacted_away")),
                  "version", "left")
              .na.fill(0, ["files_added", "files_rewritten_by_compaction", "files_removed_with_data", "files_compacted_away"])
              .orderBy("version"))
print("Every commit in the transaction log:")
per_commit.show(200, truncate=False)

# ---------- Check 1: no commit removed data ----------
data_removals = per_commit.agg(F.sum("files_removed_with_data")).first()[0] or 0
print(f"Check 1 - files removed with dataChange = true: {data_removals}  ->  {'PASS' if data_removals == 0 else 'FAIL'}")

# ---------- Check 2: every row ever written is still there ----------
rows_written = adds.filter("data_change").agg(F.sum("rows"), F.count(F.when(F.col("rows").isNull(), 1))).first()
rows_now = spark.table(TABLE).count()
if rows_written[1]:
    # The Eventstream writer does not record row counts in the log. Rows can only disappear if the file
    # holding them is removed, so check instead that every file ever added is still part of the table.
    added = {r[0].split("/")[-1] for r in log.filter(F.col("add").isNotNull() & F.coalesce("add.dataChange", F.lit(True)))
             .select("add.path").collect()}
    live = {p.split("/")[-1] for p in spark.table(TABLE).inputFiles()}
    missing = added - live if not (per_commit.agg(F.sum("files_compacted_away")).first()[0] or 0) else None
    if missing is None:
        print(f"Check 2 - {rows_written[1]} files have no row statistics and the table was compacted; "
              "Check 1 alone shows no data was removed")
    else:
        print(f"Check 2 - files added by data commits: {len(added)}, still in the table: {len(added) - len(missing)}"
              f"  ->  {'PASS' if not missing else 'FAIL'}  (row statistics missing on {rows_written[1]} files,"
              " so files are compared instead of row counts)")
else:
    print(f"Check 2 - rows written by data commits: {rows_written[0]:,}, rows in table now: {rows_now:,}"
          f"  ->  {'PASS' if rows_written[0] == rows_now else 'FAIL'}")
spark.table(TABLE).groupBy("source").count().orderBy("source").show()

# ---------- Enforce: Delta refuses deletes and updates from now on ----------
spark.sql(f"ALTER TABLE {TABLE} SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
print("delta.appendOnly =", spark.sql(f"SHOW TBLPROPERTIES {TABLE} ('delta.appendOnly')").first()["value"])
for statement in (f"DELETE FROM {TABLE} WHERE 1 = 0", f"UPDATE {TABLE} SET source = source WHERE 1 = 0"):
    try:
        spark.sql(statement)
        print(f"NOT refused: {statement}")
    except Exception as e:
        print(f"Refused as expected: {statement}\n   -> {str(e).splitlines()[0][:160]}")
print(f"Rows still in table: {spark.table(TABLE).count():,}")
# To undo (e.g. if a future writer needs it): ALTER TABLE bronze_telemetry UNSET TBLPROPERTIES ('delta.appendOnly')
