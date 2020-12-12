import csv


def parse_member_of(file_name):
    with open(file_name, 'r') as file:
        csv_reader = csv.reader(file, delimiter='\t')
        next(csv_reader)
        with open('../data/temp/membersOf.csv', 'w') as f:
            writer = csv.writer(f, delimiter='\t', lineterminator='\n')
            writer.writerow(['id', 'authors_name', 'member_of'])
            for row in csv_reader:
                if row[3] is not "":
                    writer.writerow([row[0], row[2], row[3]])


if __name__ == "__main__":
    file_name = '../data/original/authors.csv'
    parse_member_of(file_name)
