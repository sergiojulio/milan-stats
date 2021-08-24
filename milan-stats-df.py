from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

from pyspark.sql.functions import sum, col, desc

spark = SparkSession.builder.appName("ResultadosSanSiro").master("local[*]").getOrCreate()

# Create schema when reading customer-orders
resultados = StructType([\
    StructField("date", StringType(), True),
    StructField("year", StringType(), True),
    StructField("team_h", StringType(), True),
    StructField("team_v", StringType(), True),
    StructField("score", StringType(), True),
    StructField("gh", StringType(), True),
    StructField("gv", StringType(), True),
    StructField("unkwon", StringType(), True)
    ])

# Load up the data into spark dataset
resultadosDF = spark.read.schema(resultados).csv("/home/sergio/Dropbox/spark_course/italy.csv")


resultadosDF.filter(col("team_v") == 'Juventus') \
    .filter(col("team_h") == 'Bologna FC') \
    .groupBy("score").count().sort(desc("count")).show()


spark.stop()


