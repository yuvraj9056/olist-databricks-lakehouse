from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def clean_text_column(col_name):
  """Strips whitespace, replaces underscores with spaces, and applies Title Case."""
  return F.initcap(F.regexp_replace(F.trim(F.col(col_name)), "_", " "))


def generate_surrogate_key(
    df, id_col, prefix_val, key_col_name, target_table_name, spark
):
  """Generates incremental surrogate keys (e.g., 'S000001') with a specified column name (e.g., 'seller_key')

  accounting for existing max keys in the target table.
  """
  table_exists = spark.catalog.tableExists(target_table_name)

  if not table_exists:
    max_key_val = 0
  else:
    max_row = spark.sql(
        f"SELECT MAX(CAST(SUBSTRING({key_col_name}, 2) AS INT)) as max_key"
        f" FROM {target_table_name}"
    ).collect()[0]
    max_key_val = max_row["max_key"] if max_row["max_key"] is not None else 0

  # Add temporary sequence and compute new key
  df_keyed = (
      df.withColumn("temp_seq", F.row_number().over(Window.orderBy(id_col)))
      .withColumn(
          key_col_name,
          F.concat(
              F.lit(prefix_val),
              F.lpad(
                  (F.col("temp_seq") + F.lit(max_key_val)).cast("string"),
                  6,
                  "0",
              ),
          ),
      )
      .drop("temp_seq")
  )

  return df_keyed