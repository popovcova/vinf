import csv
import math
import re
from collections import Counter
from whoosh.analysis import StemmingAnalyzer
from whoosh.fields import *
import os.path
from whoosh import index
from whoosh.qparser import QueryParser
from whoosh.reading import TermNotFound
from html.parser import HTMLParser


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


def load_index(file_name, index_dir):
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)

    if index.exists_in(index_dir):
        return index.open_dir(index_dir)
    else:
        return create_index(file_name, index_dir)


def create_index(file_name, index_dir):
    schema = Schema(entity=ID(), authors_name=TEXT(analyzer=StemmingAnalyzer(), stored=True),
                    member_of=KEYWORD(stored=True))
    h = HTMLParser()
    idx = index.create_in(index_dir, schema)
    writer = idx.writer()

    with open(file_name, 'r') as file:
        csv_reader = csv.reader(file, delimiter='\t')
        next(csv_reader)

        for row in csv_reader:
            authors_name = row[1]
            authors_name = h.unescape(authors_name)
            member_of = row[2]
            writer.add_document(authors_name=f'{authors_name}', member_of=f'{member_of}')

    writer.commit()

    return idx


def find_similar_authors(file_name_index, index_dir, file_name):
    idx = load_index(file_name_index, index_dir)
    h = HTMLParser()
    with open('../datas/results/similarAuthors.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['name', 'similar_name'])
        with open(file_name, 'r') as file:
            csv_reader = csv.reader(file, delimiter='\t')
            next(csv_reader)
            for row in csv_reader:
                aff1 = row[1]
                aff2 = row[3]

                qp = QueryParser('entity', schema=idx.schema)
                query = qp.parse(f'{aff1}')
                with idx.searcher() as searcher:
                    try:
                        results = searcher.search(query)
                    except TermNotFound:
                        results = []

                    for hit in results:
                       affiliation = hit['name']

                qp = QueryParser('entity', schema=idx.schema)
                query = qp.parse(f'{aff2}')
                with idx.searcher() as searcher:
                    try:
                        results = searcher.search(query)
                    except TermNotFound:
                        results = []

                    for hit in results:
                        affiliation2 = hit['name']

                vector1 = text_to_vector(affiliation)
                vector2 = text_to_vector(affiliation2)

                similarity = get_cosine(vector1, vector2)

                if similarity >= 0.5:
                    writer.writerow([row[0], row[2]])
    f.close()


if __name__ == "__main__":
    file_name_index = '../data/original/affiliations.csv'
    index_dir = '../data/index/affiliations'
    file_name = '../data/temp/similarAuthorsPlusAffiliations.csv'

    find_similar_authors(file_name_index, index_dir, file_name)
