from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, column

# spark 실행계획 생성
df = sqlContext.read.format("json").option("compression", "gzip").load('s3://path/of/the/s3.gz')

# spark 액션
# schema 확인
df.printSchema()

# DataFrame 확인
df.show(5)

# column에서 하위 schema들도 확인 가능
df.select(col("properties.name")).show()

