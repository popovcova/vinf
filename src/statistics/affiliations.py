from src.statistics.distributed_counting import distributed_count


if __name__ == "__main__":
    file_name = '../data/original/affiliations.csv'
    distributed_count(file_name, True)
    distributed_count(file_name, False)
