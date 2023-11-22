import re
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

# column 불러오기
dfType = df.dtypes[1:]

# 정규식 표현
"""
중간에 nested column이 있어서 name:struct<con:string,term:struct<for:string>.ip:string> 식으로 표현되는거 [('name', struct), ('con' : 'string'), ('term', 'struct'), ('for', 'string'), ('ip', 'string')] << 식으로 바꿔줌
"""
def parse_struct_string(struct_str):
    pattern = re.compile(r'([a-zA-Z0-9_]+):(\b\w+\b)')
    matches = pattern.findall(struct_str)

    return matches

colName = []

for i in range(len(dfType)):
    if 'struct' in dfType[i][1]:
        for j in parse_struct_string(dfType[i][1]):
            if 'struct' in j[1]:
                for column in df.select(f"{dfType[i][0]}.{j[0]}.*").columns:
                    colName.append(f"{dfType[i][0]}.{column}.{j[0]}")
            else:
                for column in df.select(f"{dfType[i][0]}.*").columns:
                    colName.append(f"{dfType[i][0]}.{column}")
    else:
        colName.append(dfType[i][0])

colName
