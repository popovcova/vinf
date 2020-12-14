import os.path
import csv

from html.parser import HTMLParser
from whoosh.analysis import StemmingAnalyzer
from whoosh.fields import *
from whoosh.qparser import QueryParser
from whoosh import index, scoring
from whoosh.reading import TermNotFound


def load_index(file_name, index_dir):
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)

    if index.exists_in(index_dir):
        return index.open_dir(index_dir)
    else:
        return create_index(file_name, index_dir)


def create_index(file_name, index_dir):
    schema = Schema(entity=ID(), rank=NUMERIC(), name=TEXT(analyzer=StemmingAnalyzer(), stored=True),
                    paper_count=NUMERIC(), citation_count=NUMERIC(), created=DATETIME())

    idx = index.create_in(index_dir, schema)
    writer = idx.writer()

    h = HTMLParser()

    with open(file_name, 'r') as file:
        csv_reader = csv.reader(file, delimiter='\t')
        next(csv_reader)

        for row in csv_reader:
            name = h.unescape(row[2])
            writer.add_document(name=f'{name}')

    writer.commit()

    return idx


def find_name(file_name, index_dir, squery):
    idx = load_index(file_name, index_dir)
    qp = QueryParser('name', schema=idx.schema)
    query = qp.parse(squery)

    with idx.searcher(weighting=scoring.TF_IDF()) as searcher:
        try:
            corrected_term = searcher.correct_query(query, squery)
            if corrected_term.query != query:
                print("Did you mean:", corrected_term.string)
            results = searcher.search(query, limit=10)
        except TermNotFound:
            results = []

        print("{}".format(len(results)))
        for hit in results:
            print("  * {}".format(hit['name']))


if __name__ == "__main__":
    file_name = '../data/original/conferenceSeries.csv'
    index_dir = '../data/index/conferenceSeries'
    query = input()
    find_name(file_name, index_dir, query)
