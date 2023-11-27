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
segment_datatypes = df.dtypes[1:]

segment_datatypes = [('datatype', 'string'),
                     ('text', 'struct<datatype:struct<datatype:string,datatype:string>,datatype:string,datatype:struct<datatype:string>,datatype:string>'),
                     ('ent', 'string'), ('mesd', 'string'),
                     ('pop', 'struct<cat:string,pat:string,pop:struct<datatype:struct<sot:string>,rel:string>,ref:string>'),
                     ('tray', 'struct<Id:string>')]

import re

schema = []

def properties_datatype(nested_col):
    col = []
    _full_field = nested_col[:nested_col.find("<")] + nested_col[nested_col.rfind(">")+1:]
    full_field = _full_field.split(',')
    for _col in full_field:
        col.append(_col.split(':')[0])
    nes_col = nested_col[nested_col.find("<")+1:nested_col.rfind(">")]
    return col, full_field, nes_col

for schema_name, schema_datatype in segment_datatypes:
    if schema_name == 'text':
        schema_datatype = schema_datatype[schema_datatype.find('<')+1:]
        matches = re.findall(r'[^,<>]+<[^<>]+>|[^,<>]+', schema_datatype)
        for matche in matches:
            if 'struct' in matche:
                up_schema = matche[:matche.find(':')]
                down_schema_ls = matche[matche.find('<')+1:matche.find('>')]
                for _down_schema in down_schema_ls.split(','):
                    down_schema = _down_schema[:_down_schema.find(':')]
                    schema.append(f'{schema_name}.{up_schema}.{down_schema}')
            else:
                up_schema = matche[:matche.find(':')]
                schema.append(f'{schema_name}.{up_schema}')
    
    elif schema_name == 'pop':
        if 'struct' in schema_datatype:
            schema_datatype = schema_datatype[schema_datatype.find("<")+1:schema_datatype.rfind(">")]
            col, full_field, nes_col = properties_datatype(schema_datatype)
            for i in range(len(col)):
                if 'struct' in full_field[i]:
                    col_1, full_field_1, nes_col_1 = properties_datatype(nes_col)
                    for j in range(len(col_1)):
                        if 'struct' in full_field_1[j]:
                            nes_col_1_ls = nes_col_1.split(',')
                            for k in nes_col_1_ls:
                                nes_col_1_ls = k[:k.find(':')]
                                schema.append(f'{schema_name}.{col[i]}.{col_1[j]}.{nes_col_1_ls}')
                        else:
                            schema.append(f'{schema_name}.{col[i]}.{col_1[j]}')
                else:
                    schema.append(f'{schema_name}.{col[i]}')
    
    elif schema_name == 'tray':
        schema.append(f'{schema_name}.Id')
    
    else:
        schema.append(schema_name)