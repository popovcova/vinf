from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


def distributed_count(file, citation):
    spark = SparkSession.builder.master("local").getOrCreate()
    df = spark.read.options(header='True', delimiter='\t').csv(file)

    df = df.withColumn("citation_count", df["citation_count"].cast(IntegerType()))
    df = df.withColumn("reference_count", df["reference_count"].cast(IntegerType()))

    if citation:
        print("Order by citation count:")
        df.orderBy('citation_count', ascending=False).show()
    else:
        print("Order by reference count:")
        df.orderBy('reference_count', ascending=False).show()
    spark.stop()


if __name__ == "__main__":
    file_name = '../data/original/papers.csv'
    distributed_count(file_name, True)
    distributed_count(file_name, False)
