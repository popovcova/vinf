import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover


def count_words(file, column_name):
    spark = SparkSession.builder.master("local").getOrCreate()
    df = spark.read.options(header='True', delimiter='\t').csv(file)

    df.withColumn('word', f.explode(f.split(f.col(column_name), ' '))) \
        .groupBy('word') \
        .count() \
        .sort('count', ascending=False)

    tokenizer = Tokenizer(inputCol=column_name, outputCol="words_token")
    tokenized = tokenizer.transform(df).select('words_token')

    remover = StopWordsRemover(inputCol='words_token', outputCol='words_clean')
    data_clean = remover.transform(tokenized).select('words_clean')

    result = data_clean.withColumn('word', f.explode(f.col('words_clean'))) \
        .groupBy('word') \
        .count().sort('count', ascending=False)

    result.show()

    spark.stop()


if __name__ == "__main__":
    file = "../data/original/papers.csv"
    column = 'title'
    count_words(file, column)
