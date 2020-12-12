import csv
from src.statistics.distributedCounting import distributed_count


def sort_by_citation_count(file_name):
    with open(file_name, 'r') as file:
        csv_reader = csv.reader(file, delimiter='\t')
        next(csv_reader)

        fields = {}
        for row in csv_reader:
            fields[f'{row[2]}'] = int(row[6])

    fields = dict(sorted(fields.items(), key=lambda item: item[1], reverse=True))

    with open('../data/statistics/fieldsOfStudyCitationCount.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['fieldsOfStudy', 'citation_count'])
        for key, value in fields.items():
            writer.writerow([key, value])


def sort_by_paper_count(file_name):
    with open(file_name, 'r') as file:
        csv_reader = csv.reader(file, delimiter='\t')
        next(csv_reader)

        fields = {}
        for row in csv_reader:
            fields[f'{row[2]}'] = int(row[5])

    fields = dict(sorted(fields.items(), key=lambda item: item[1], reverse=True))

    with open('../data/statistics/fieldsOfStudyPaperCount.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['fieldsOfStudy', 'paper_count'])
        for key, value in fields.items():
            writer.writerow([key, value])


if __name__ == "__main__":
    file_name = '../data/original/fieldsOfStudy.csv'
    # sort_by_paper_count(file_name)
    # sort_by_citation_count(file_name)
    distributed_count(file_name, True)
    distributed_count(file_name, False)
