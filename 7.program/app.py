import pyspark
from pyspark.sql import SparkSession spark=SparkSession.builder.appName('housing_price_model').getOrCreate()
df=spark.read.csv('/content/drive/MyDrive/cruise_ship_info.csv',inferSchema=True,header=True) df.show(10)
df.printSchema()

df.columns
from pyspark.ml.feature import StringIndexer 
indexer=StringIndexer(inputCol='Cruise_line',outputCol='cruise_cat') 
indexed=indexer.fit(df).transform(df)

for item in indexed.head(5):
    print(item) 
    print('\n')

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

assembler=VectorAssembler(inputCols=['Age',
'Tonnage',
'passengers',
'length',
'cabins',
'passenger_density', 'cruise_cat'],outputCol='features')
output=assembler.transform(indexed) output.select('features','crew').show(5)

final_data=output.select('features','crew')

train_data,test_data=final_data.randomSplit([0.7,0.3]) 
train_data.describe().show()
test_data.describe().show()

from pyspark.ml.regression import LinearRegression
ship_lr=LinearRegression(featuresCol='features',labelCol='crew')
trained_ship_model=ship_lr.fit(train_data)
ship_results=trained_ship_model.evaluate(train_data) 
print('Rsquared Error :',ship_results.r2)
unlabeled_data=test_data.select('features')
unlabeled_data.show(5) 
predictions=trained_ship_model.transform(unlabeled_data) 
predictions.show()
