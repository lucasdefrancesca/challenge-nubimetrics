import findspark; findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import when

spark = SparkSession.builder                 \
                    .master("local")         \
                    .appName("nubimetrics")  \
                    .getOrCreate()
   
df = spark.read.json("./ImputEjemplos/Sellers.json")
df.printSchema()

df_ = df.select(F.expr("body.site_id").alias("SiteId"),
                F.expr("body.id").alias("SellerId"),
                F.expr("body.nickname").alias("SellerNickname"),
                F.expr("body.points").alias("SellerPoints")
                )

df_.printSchema()
df_.show()

dff = df_.withColumn('SellerPoints',               \
          when(df_.SellerPoints > 0 , 'Positivo')  \
         .when(df_.SellerPoints == 0, 'Cero')      \
         .when(df_.SellerPoints < 0, 'Negativo'))

# Duda sobre el patron del archivo
dff.write.partitionBy("SellerPoints") \
         .mode('append')              \
         .option('header', True)      \
         .format("csv")               \
         .save('challenges/MPE/2020/08/28/')

# Close spark session
spark.stop()
