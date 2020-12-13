import csv
from pyspark.sql import SparkSession


def count_languages(file_name):
    with open(file_name, 'r') as file:
        csv_reader = csv.reader(file, delimiter='\t')
        next(csv_reader)

        languages = {}
        for row in csv_reader:
            language = row[1]

            if languages.get(language) is None:
                languages[f'{language}'] = 1
            else:
                languages[f'{language}'] += 1

        print(languages)

    languages = dict(sorted(languages.items(), key=lambda item: item[1], reverse=True))

    with open('../data/statistics/languageCounts.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['language', 'count'])
        for key, value in languages.items():
            writer.writerow([key, value])


def distributed_count(file):
    spark = SparkSession.builder.master("local").getOrCreate()
    df = spark.read.options(header='True', delimiter='\t').csv(file)
    df.groupBy("language").count().orderBy('count', ascending=False).show()
    spark.stop()


if __name__ == "__main__":
    file_name = '../data/paperLanguages.csv'
    # count_languages(file_name)
    distributed_count(file_name)
