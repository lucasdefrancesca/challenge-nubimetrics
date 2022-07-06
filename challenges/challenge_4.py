import findspark; findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, arrays_zip, row_number


spark = SparkSession.builder                 \
                    .master("local")         \
                    .appName("nubimetrics")  \
                    .getOrCreate()
   
df = spark.read.json("./ImputEjemplos/MPE1004.json")
df.printSchema()

df_ = df.select(F.expr("results.id").alias("itemId"),
                F.expr("results.available_quantity").alias("availableQuantity"),
                F.expr("results.sold_quantity").alias("soldQuantity")
                )

df_tmp = df_.withColumn("tmp", F.explode(F.arrays_zip("itemId", "availableQuantity", "soldQuantity"))) \
            .select("tmp.ItemId", "tmp.soldQuantity", "tmp.availableQuantity")

window  = Window.orderBy("itemId")

dff = df_tmp.withColumn("rowId", row_number().over(window))
dff.show(n=50)                        

# Close spark session
spark.stop()
