import csv
import math
import re
from collections import Counter
from html.parser import HTMLParser
from sklearn.metrics.pairwise import cosine_similarity


WORD = re.compile(r"\w+")


def get_cosine(vec1, vec2):
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])

    sum1 = sum([vec1[x] ** 2 for x in list(vec1.keys())])
    sum2 = sum([vec2[x] ** 2 for x in list(vec2.keys())])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    else:
        return float(numerator) / denominator


def text_to_vector(text):
    words = WORD.findall(text)
    return Counter(words)


def find_similar(file_name):
    h = HTMLParser()
    with open('./data/results/similarAuthorsNames.csv', 'w') as output:
        writer = csv.writer(output, delimiter='\t')
        writer.writerow(['id', 'name', 'similar_id', 'similar_name', 'similarity'])
        with open(file_name, 'r') as file:
            csv_reader = csv.reader(file, delimiter='\t')
            next(csv_reader)

            for row in csv_reader:
                with open(file_name, 'r') as f:
                    csv_reader2 = csv.reader(f, delimiter='\t')
                    next(csv_reader2)

                    for row2 in csv_reader2:
                        name = str(row[1])
                        name2 = str(row2[1])

                        name = h.unescape(name)
                        name2 = h.unescape(name2)

                        vector1 = text_to_vector(name)
                        vector2 = text_to_vector(name2)

                        similarity = get_cosine(vector1, vector2)

                        if row[0] != row2[0] and similarity >= 0.75:
                            print(name, name2, similarity)
                            writer.writerow([row[0], name, row2[0], name2, similarity])


if __name__ == "__main__":
    file_name = '../data/temp/membersOf.csv'
    find_similar(file_name)
