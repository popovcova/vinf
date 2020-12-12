from src.statistics.distributedCounting import distributed_count


if __name__ == "__main__":
    file_name = '../data/original/authors.csv'
    distributed_count(file_name, True)
    distributed_count(file_name, False)
