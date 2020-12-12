import csv
from itertools import count

from whoosh.analysis import StemmingAnalyzer
from whoosh.fields import *
import os.path
from whoosh import index
from whoosh.qparser import QueryParser
from whoosh import scoring
from whoosh.reading import TermNotFound
from html.parser import HTMLParser


def load_index(file_name, index_dir):
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)

    if index.exists_in(index_dir):
        return index.open_dir(index_dir)
    else:
        return create_index(file_name, index_dir)


def create_index(file_name, index_dir):
    schema = Schema(entity=KEYWORD(), authors_name=TEXT(analyzer=StemmingAnalyzer(), stored=True),
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


def search_similar_names(file_name, index_dir, output_file):
    idx = load_index(file_name, index_dir)
    h = HTMLParser()
    pairs = []
    count = 0
    with open(output_file, 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['name', 'affiliation', 'similar_name', 'similar_affiliation'])
    f.close()
    with open(file_name, 'r') as file:
        csv_reader = csv.reader(file, delimiter='\t')
        next(csv_reader)
        for row in csv_reader:
            name = row[1]
            name = h.unescape(name)

            qp = QueryParser('authors_name', schema=idx.schema)
            query = qp.parse(f'{name}')

            with idx.searcher(weighting=scoring.TF_IDF()) as searcher:
                try:
                    results = searcher.search(query)
                except TermNotFound:
                    results = []

                if len(results) > 1:

                    for hit in results:
                        pair = [name.encode('utf8'), row[2], hit['authors_name'].encode('utf8'), hit['member_of']]
                        pairs.append(pair)
                        count += 1

            if count >= 500:
                with open(output_file, 'a+') as f:
                    writer = csv.writer(f, delimiter='\t', lineterminator='\n')
                    for i in range(len(pairs)):
                        writer.writerow([pairs[i][0], pairs[i][1], pairs[i][2], pairs[i][3]])
                    pairs = []
                    count = 0
                f.close()
    if len(pairs) >= 0:
        with open(output_file, 'a+') as f:
            writer = csv.writer(f, delimiter='\t', lineterminator='\n')
            for i in range(len(pairs)):
                writer.writerow([pairs[i][0], pairs[i][1], pairs[i][2], pairs[i][3]])

        f.close()


if __name__ == "__main__":
    file_name = '../data/temp/membersOf.csv'
    index_dir = '../data/index/membersOf'
    output = '../data/temp/similarAuthorsPlusAffiliations.csv'
    search_similar_names(file_name, index_dir, output)
