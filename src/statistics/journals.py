import csv
from src.statistics.distributedCounting import distributed_count


def sort_by_citation_count(file_name):
    with open(file_name, 'r') as file:
        csv_reader = csv.reader(file, delimiter='\t')
        next(csv_reader)

        journals = {}
        for row in csv_reader:
            journals[f'{row[2]}'] = int(row[5])

    journals = dict(sorted(journals.items(), key=lambda item: item[1], reverse=True))

    with open('../data/statistics/journalsCitationCount.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['journal_name', 'citation_count'])
        for key, value in journals.items():
            writer.writerow([key, value])


def sort_by_paper_count(file_name):
    with open(file_name, 'r') as file:
        csv_reader = csv.reader(file, delimiter='\t')
        next(csv_reader)

        journals = {}
        for row in csv_reader:
            journals[f'{row[2]}'] = int(row[4])

    journals = dict(sorted(journals.items(), key=lambda item: item[1], reverse=True))

    with open('../data/statistics/journalsPaperCount.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['journal_name', 'paper_count'])
        for key, value in journals.items():
            writer.writerow([key, value])


if __name__ == "__main__":
    file_name = '../data/original/journals.csv'
    # sort_by_paper_count(file_name)
    # sort_by_citation_count(file_name)
    distributed_count(file_name, True)
    distributed_count(file_name, False)
