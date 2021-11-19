
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from kafka import KafkaConsumer
##import pandas as pd
def process_msg(msg):
    print(msg.offset)
    print(json.loads(msg.value))
spark = (SparkSession
         .builder
         .master('local')
         .appName('Cards')
         #Add kafka package
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
c = KafkaConsumer('Cards', bootstrap_servers=['kafka:9093'], api_version=(2,6,0))
schema_monster = StructType([
  StructField('atk', LongType(), True),
  StructField('attribute', StringType(), True),
  StructField('card_sets',ArrayType(MapType(StringType(),StringType(),True),True),True),
  StructField('def', LongType(), True),
  StructField('id', LongType(), True),
  StructField('level', LongType(), True),
  StructField('name', StringType(), True),
  StructField('race', StringType(), True),
  StructField('type', StringType(), True),
  ])
schema_magic_trap = StructType([
  StructField('card_prices',ArrayType(MapType(StringType(),StringType(),True),True),True),
  StructField('card_sets',ArrayType(MapType(StringType(),StringType(),True),True),True),
  StructField('id', LongType(), True),
  StructField('name', StringType(), True),
  StructField('race', StringType(), True),
  StructField('type', StringType(), True),
  ])
df_monster=spark.createDataFrame([], schema_monster)
df_magic_trap=spark.createDataFrame([], schema_magic_trap)
count=0
for msg in c :
    count+=1
    data=[json.loads(msg.value)]
    df= spark.createDataFrame(data)
    d1=df.drop('archetype','desc','card_images','scale','banlist_info','format','sort','misc','staple','has_effect')
    if (StructField("atk",LongType(),True) in d1.schema):
        df_monster=df_monster.union(d1.drop('card_prices'))
    else:
        df_magic_trap=df_magic_trap.union(d1)
    df_print_MT=df_magic_trap.drop('card_prices','card_sets')
    df_print_M=df_monster.drop('card_sets')
    print("Number of Magic_Trap cards in dataframe: "+ str(df_print_MT.distinct().count()))
    print("Number of Monster cards in dataframe: "+ str(df_print_M.distinct().count()))
    if (count > 10) :
        count=5
        goats=df_monster.sort(df_monster.atk.desc())
        print('les Monstres du plus fort au plus faible en atk :')
        goats.select('name','atk').show(truncate=False)
        print('Les niveaux des monstres presents dans le dataframe :')
        df_monster.groupBy("level").count().show()
        print('l atk moyenne des monstres selon leur niveau :')
        df_monster.groupBy("level").mean("atk").show()
        print('Les attribute des monstres presents dans le dataframe :')
        df_monster.groupBy("attribute").count().show()
        print('la def moyenne des monstres selon leur attribute :')
        df_monster.groupBy("attribute").mean('def').show()
        print('les prix des cartes magie/piege :')
        df_magic_trap.select(df_magic_trap.name,explode(df_magic_trap.card_prices)).show(truncate=False)
        df_m_sets=df_monster.select(df_monster.name,explode(df_monster.card_sets))
        secret_rare_monster=df_m_sets.filter(df_m_sets.col['set_rarity']=='Secret Rare')
        print('Les cartes monstres Secret rare sont :')
        secret_rare_monster.select("name").show(truncate=False)
        df_mt_sets=df_magic_trap.select(df_magic_trap.name,explode(df_magic_trap.card_sets))
        secret_rare_magic_trap=df_mt_sets.filter(df_mt_sets.col['set_rarity']=='Secret Rare')
        print('Les cartes magie/piege Secret rare sont :')
        secret_rare_magic_trap.select("name").show(truncate=False)
print("fin")
