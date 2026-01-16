# Databricks notebook source
def merge_changes(
        batch_df: DataFrame, id_column: str, target_table: str, order_column: str
    ) -> None:
        """Merge logic for updating the incremental SCD type 2 table in bronze"""
        batch_df = deduplicate(batch_df, id_column, order_column)
        df_target = DeltaTable.forName(batch_df.sparkSession, target_table)
        target_df = spark.table(target_table).where(col("end_timestamp").isNull())
        now = datetime.now()

        batch_df = batch_df.withColumnsRenamed(
            {f"{column}": f"{column}_incoming" for column in target_df.columns}
        )
        batch_df = batch_df.alias("i").join(
            target_df.alias("t"),
            col(f"t.{id_column}") == col(f"i.{id_column}_incoming"),
            how="left",
        )
        batch_df = batch_df.withColumn("start_timestamp", lit(now))
        batch_df = batch_df.withColumn("end_timestamp", lit(None).cast("timestamp"))
        insert_df = batch_df.where(
            (col(f"t.{id_column}").isNull())
            | (
                (col(f"t.{id_column}").isNotNull())
                & (col("i.modifiedon_incoming") > col("t.modifiedon"))
            )
        ).withColumn("type", lit("insert"))
        update_df = batch_df.where(col(f"t.{id_column}").isNotNull()).withColumn(
            "type", lit("update")
        )
        changes_df = insert_df.union(update_df)

        merge_condition = (
            f"target.{id_column} = changes.{id_column}_incoming "
            f"AND target.end_timestamp IS NULL AND changes.type = 'update'"
        )
        df_target.alias("target").merge(
            changes_df.alias("changes"),
            merge_condition,
        ).whenMatchedUpdate(
            condition=col("changes.modifiedon_incoming") > col("target.modifiedon"),
            set={"end_timestamp": lit(now)},
        ).whenNotMatchedInsert(
            values={
                column: col(f"changes.{column}_incoming")
                for column in target_df.columns
                if column not in ["start_timestamp", "end_timestamp"]
            }
            | {"start_timestamp": "start_timestamp", "end_timestamp": "end_timestamp"}
        ).execute()
