from src.statistics.distributed_counting import distributed_count
from pyspark.sql import SparkSession


def count_authors(file):
    spark = SparkSession.builder.master("local").getOrCreate()
    df = spark.read.options(header='True', delimiter='\t').csv(file)
    df2 = spark.read.options(header='True', delimiter='\t').csv("../csv_data/results/membersOf.csv")
    df2 = df2.withColumnRenamed("name", "author_name")

    df_joined = df.join(df2, df.entity == df2.member_of, 'inner')

    df_joined.groupBy('name') \
        .count() \
        .sort('count', ascending=False)\
        .show()


if __name__ == "__main__":
    file_name = '../data/original/affiliations.csv'
    distributed_count(file_name, True)
    distributed_count(file_name, False)
    count_authors(file_name)
