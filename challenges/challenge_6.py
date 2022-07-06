import findspark; findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, arrays_zip, row_number, col, round, desc


spark = SparkSession.builder                 \
                    .master("local")         \
                    .appName("nubimetrics")  \
                    .getOrCreate()
   
df = spark.read.json("./ImputEjemplos/MPE1004.json")
df_visits = spark.read.option("header", True).csv("./ImputEjemplos/visits.csv")

df_ = df.select(F.expr("results.id").alias("itemId"),
                F.expr("results.available_quantity").alias("availableQuantity"),
                F.expr("results.sold_quantity").alias("soldQuantity")
                )

df_tmp = df_.withColumn("tmp", F.explode(F.arrays_zip("itemId", "availableQuantity", "soldQuantity"))) \
            .select("tmp.ItemId", "tmp.soldQuantity", "tmp.availableQuantity")

window  = Window.orderBy("itemId")
dff = df_tmp.withColumn("rowId", row_number().over(window))

# Join
df_4 = dff.select("itemId", "soldQuantity")
df_join = df_4.join(df_visits, ["itemId"])

# Filter
df_without_sold = df_join.filter(df_join.soldQuantity != 0)

# Rate and rating
window  = Window.orderBy(desc("conversionRate"))

dff = df_without_sold \
        .withColumn("conversionRate", round(col("soldQuantity") / col("visits"), 4)) \
        .withColumn("conversionRanking", row_number().over(window))

dff.show()

# Close spark session
spark.stop()
