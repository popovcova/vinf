from bz2 import BZ2File as bzopen
import csv
import re


def parse_affiliations(file_name):
    entity = None
    rank = None
    name = None
    grid = None
    paper_count = None
    citation_count = None

    with open('../data/original/affiliations.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['entity', 'rank', 'name', 'grid', 'paper_count', 'citation_count'])

        with bzopen(file_name, "r") as file:

            for line in file:
                decoded_line = line.decode("utf-8")

                new_entity = None
                new_entity = re.search(r'\/class\/Affiliation\>', decoded_line)
                if new_entity is not None:
                    if entity is not None:
                        writer.writerow([entity, rank, name, grid, paper_count, citation_count])

                    entity = None
                    rank = None
                    name = None
                    grid = None
                    paper_count = None
                    citation_count = None

                # parse entity
                entity_url = None
                entity_url = re.search(r'^\<http\:\/\/ma\-graph\.org\/entity\/[0-9]*', decoded_line)
                entity_url = str(entity_url.group(0))
                entity = entity_url.rsplit('/', 1)[1]

                # parse rank
                rank_presence = None
                rank_presence = re.search(r'\/property\/rank\> ', decoded_line)
                if rank_presence is not None:
                    rank_line = re.search(r'"[0-9]*"', decoded_line)
                    rank = str(rank_line.group(0))
                    rank = rank.split('"', 1)[1]
                    rank = rank.split('"', 1)[0]

                # parse name
                name_presence = None
                name_presence = re.search(r'\/name\> ', decoded_line)
                if name_presence is not None:
                    name_line = re.search(r'.*', decoded_line)
                    name = str(name_line.group(0))
                    name = name.split('"', 1)[1]
                    name = name.split('"', 1)[0]

                # parse grid
                grid_presence = None
                grid_presence = re.search(r'\/property\/grid\> ', decoded_line)
                if grid_presence is not None:
                    grid_line = re.search(r'grid\.[0-9]*\.[0-9]*[a-z]*>', decoded_line)
                    grid = str(grid_line.group(0))
                    grid = grid[:-1]

                # parse paper_count
                paper_count_presence = None
                paper_count_presence = re.search(r'\/property\/paperCount\> ',
                                                 decoded_line)
                if paper_count_presence is not None:
                    paper_count_line = re.search(r'"[0-9]+"', decoded_line)
                    paper_count = str(paper_count_line.group(0))
                    paper_count = paper_count.split('"', 1)[1]
                    paper_count = paper_count.split('"', 1)[0]

                # parse citation_count
                citation_count_presence = None
                citation_count_presence = re.search(r'\/property\/citationCount\> ',
                                                    decoded_line)
                if citation_count_presence is not None:
                    citation_count_line = re.search(r'"[0-9]+"', decoded_line)
                    citation_count = str(citation_count_line.group(0))
                    citation_count = citation_count.split('"', 1)[1]
                    citation_count = citation_count.split('"', 1)[0]


def parse_authors(file_name):

    entity = None
    rank = None
    name = None
    member_of = None
    paper_count = None
    citation_count = None
    creation_date = None

    with open('../data/original/authors.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['entity', 'rank', 'name', 'member_of', 'paper_count', 'citation_count', 'created'])

        with bzopen(file_name, "r") as file:

            for line in file:
                decoded_line = line.decode("utf-8")

                new_entity = None
                new_entity = re.search(r'\/class\/Author\>', decoded_line)
                if new_entity is not None:
                    if entity is not None:
                        writer.writerow([entity, rank, name, member_of, paper_count, citation_count, creation_date])

                    entity = None
                    rank = None
                    name = None
                    member_of = None
                    paper_count = None
                    citation_count = None
                    creation_date = None

                # parse entity
                entity_url = None
                entity_url = re.search(r'^\<http\:\/\/ma\-graph\.org\/entity\/[0-9]*', decoded_line)
                entity_url = str(entity_url.group(0))
                entity = entity_url.rsplit('/', 1)[1]

                # parse rank
                rank_presence = None
                rank_presence = re.search(r'\/property\/rank\> ', decoded_line)
                if rank_presence is not None:
                    rank_line = re.search(r'"[0-9]*"', decoded_line)
                    rank = str(rank_line.group(0))
                    rank = rank.split('"', 1)[1]
                    rank = rank.split('"', 1)[0]

                # parse name
                name_presence = None
                name_presence = re.search(r'\/name\> ', decoded_line)
                if name_presence is not None:
                    name_line = re.search(r'.*', decoded_line)
                    name = str(name_line.group(0))
                    name = name.split('"', 1)[1]
                    name = name.split('"', 1)[0]

                # parse member_of
                member_of_presence = None
                member_of_presence = re.search(r'\/org\#memberOf\> ', decoded_line)
                if member_of_presence is not None:
                    member_of_line = re.search(r'\<http\:/\/ma\-graph\.org\/entity\/[0-9]*\> \.', decoded_line)
                    member_of = str(member_of_line.group(0))
                    member_of = member_of.rsplit('/', 1)[1]
                    member_of = member_of.split('>', 1)[0]

                # parse paper_count
                paper_count_presence = None
                paper_count_presence = re.search(r'\/property\/paperCount\> ',
                                                 decoded_line)
                if paper_count_presence is not None:
                    paper_count_line = re.search(r'"[0-9]+"', decoded_line)
                    paper_count = str(paper_count_line.group(0))
                    paper_count = paper_count.split('"', 1)[1]
                    paper_count = paper_count.split('"', 1)[0]

                # parse citation_count
                citation_count_presence = None
                citation_count_presence = re.search(r'\/property\/citationCount\> ',
                                                    decoded_line)
                if citation_count_presence is not None:
                    citation_count_line = re.search(r'"[0-9]+"', decoded_line)
                    citation_count = str(citation_count_line.group(0))
                    citation_count = citation_count.split('"', 1)[1]
                    citation_count = citation_count.split('"', 1)[0]

                # parse creation_date
                creation_date_presence = None
                creation_date_presence = re.search(r'\/dc\/terms\/created\> ', decoded_line)
                if creation_date_presence is not None:
                    creation_date_line = re.search(r'[0-9]{4}\-(0|1)[0-9]\-[0-3][0-9]', decoded_line)
                    creation_date = str(creation_date_line.group(0))


def parse_conference_instances(file_name):

    entity = None
    name = None
    location = None
    start = None
    end = None
    paper_count = None
    citation_count = None
    creation_date = None

    with open('../data/original/conferenceInstances.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['entity', 'name', 'location', 'start', 'end', 'paper_count', 'citation_count', 'created'])

        with bzopen(file_name, "r") as file:
            for line in file:
                decoded_line = line.decode("utf-8")

                new_entity = None
                new_entity = re.search(r'\/class\/ConferenceInstance\>', decoded_line)
                if new_entity is not None:
                    if entity is not None:
                        writer.writerow(
                            [entity, name, location, start, end, paper_count, citation_count, creation_date])
                        print(entity, name, location, start, end, paper_count, citation_count, creation_date)

                    entity = None
                    name = None
                    location = None
                    start = None
                    end = None
                    paper_count = None
                    citation_count = None
                    creation_date = None

                # parse entity
                entity_url = None
                entity_url = re.search(r'^\<http\:\/\/ma\-graph\.org\/entity\/[0-9]*', decoded_line)
                entity_url = str(entity_url.group(0))
                entity = entity_url.rsplit('/', 1)[1]

                # parse name
                name_presence = None
                name_presence = re.search(r'\/name\> ', decoded_line)
                if name_presence is not None:
                    name_line = re.search(r'"[0-9]*"', decoded_line)
                    name = str(name_line.group(0))
                    name = name.split('"', 1)[1]
                    name = name.split('"', 1)[0]

                # parse location
                location_presence = None
                location_presence = re.search(r'\/ontology\/location\> ', decoded_line)
                if location_presence is not None:
                    location_line = re.search(r'\<http\:\/\/dbpedia\.org\/resource\/.*>', decoded_line)
                    location = str(location_line.group(0))
                    location = location.rsplit('/', 1)[1]
                    location = location[:-1]
                    location = location.replace("_", " ")

                # parse start
                start_date_presence = None
                start_date_presence = re.search(r'\/timeline\.owl\#start\> ',
                                                decoded_line)
                if start_date_presence is not None:
                    start_date_line = re.search(r'[0-9]{4}\-(0|1)[0-9]\-[0-3][0-9]', decoded_line)
                    start = str(start_date_line.group(0))

                # parse end
                end_date_presence = None
                end_date_presence = re.search(r'\/timeline\.owl\#end\> ',
                                              decoded_line)
                if end_date_presence is not None:
                    end_date_line = re.search(r'[0-9]{4}\-(0|1)[0-9]\-[0-3][0-9]', decoded_line)
                    end = str(end_date_line.group(0))

                # parse paper_count
                paper_count_presence = None
                paper_count_presence = re.search(r'\/property\/paperCount\> ',
                                                 decoded_line)
                if paper_count_presence is not None:
                    paper_count_line = re.search(r'"[0-9]+"', decoded_line)
                    paper_count = str(paper_count_line.group(0))
                    paper_count = paper_count.split('"', 1)[1]
                    paper_count = paper_count.split('"', 1)[0]

                # parse citation_count
                citation_count_presence = None
                citation_count_presence = re.search(r'\/property\/citationCount\> ',
                                                    decoded_line)
                if citation_count_presence is not None:
                    citation_count_line = re.search(r'"[0-9]+"', decoded_line)
                    citation_count = str(citation_count_line.group(0))
                    citation_count = citation_count.split('"', 1)[1]
                    citation_count = citation_count.split('"', 1)[0]

                # parse creation_date
                creation_date_presence = None
                creation_date_presence = re.search(r'\/dc\/terms\/created\> ', decoded_line)
                if creation_date_presence is not None:
                    creation_date_line = re.search(r'[0-9]{4}\-(0|1)[0-9]\-[0-3][0-9]', decoded_line)
                    creation_date = str(creation_date_line.group(0))


def parse_conference_series(file_name):

    entity = None
    rank = None
    name = None
    paper_count = None
    citation_count = None
    creation_date = None

    with open('../data/original/conferenceSeries.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['entity', 'rank', 'name', 'paper_count', 'citation_count', 'created'])

        with bzopen(file_name, "r") as file:
            for line in file:
                decoded_line = line.decode("utf-8")

                new_entity = None
                new_entity = re.search(r'\/class\/ConferenceSeries\>', decoded_line)
                if new_entity is not None:
                    if entity is not None:
                        writer.writerow([entity, rank, name, paper_count, citation_count, creation_date])

                    entity = None
                    rank = None
                    name = None
                    paper_count = None
                    citation_count = None
                    creation_date = None

                # parse entity
                entity_url = None
                entity_url = re.search(r'^\<http\:\/\/ma\-graph\.org\/entity\/[0-9]*', decoded_line)
                entity_url = str(entity_url.group(0))
                entity = entity_url.rsplit('/', 1)[1]

                # parse rank
                rank_presence = None
                rank_presence = re.search(r'\/property\/rank\> ', decoded_line)
                if rank_presence is not None:
                    rank_line = re.search(r'"[0-9]*"', decoded_line)
                    rank = str(rank_line.group(0))
                    rank = rank.split('"', 1)[1]
                    rank = rank.split('"', 1)[0]

                # parse name
                name_presence = None
                name_presence = re.search(r'.\/name\> ', decoded_line)
                if name_presence is not None:
                    name_line = re.search(r'.*', decoded_line)
                    name = str(name_line.group(0))
                    name = name.split('"', 1)[1]
                    name = name.split('"', 1)[0]

                # parse paper_count
                paper_count_presence = None
                paper_count_presence = re.search(r'.\/property\/paperCount\> ',
                                                 decoded_line)
                if paper_count_presence is not None:
                    paper_count_line = re.search(r'"[0-9]+"', decoded_line)
                    paper_count = str(paper_count_line.group(0))
                    paper_count = paper_count.split('"', 1)[1]
                    paper_count = paper_count.split('"', 1)[0]

                # parse citation_count
                citation_count_presence = None
                citation_count_presence = re.search(r'\/property\/citationCount\> ',
                                                    decoded_line)
                if citation_count_presence is not None:
                    citation_count_line = re.search(r'"[0-9]+"', decoded_line)
                    citation_count = str(citation_count_line.group(0))
                    citation_count = citation_count.split('"', 1)[1]
                    citation_count = citation_count.split('"', 1)[0]

                # parse creation_date
                creation_date_presence = None
                creation_date_presence = re.search(r'\/dc\/terms\/created\> ', decoded_line)
                if creation_date_presence is not None:
                    creation_date_line = re.search(r'[0-9]{4}\-(0|1)[0-9]\-[0-3][0-9]', decoded_line)
                    creation_date = str(creation_date_line.group(0))


def parse_journals(file_name):

    entity = None
    rank = None
    name = None
    issn = None
    paper_count = None
    citation_count = None
    creation_date = None

    with open('../data/original/journals.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['entity', 'rank', 'name', 'issn', 'paper_count', 'citation_count', 'created'])

        with bzopen(file_name, "r") as file:
            for line in file:
                decoded_line = line.decode("utf-8")

                new_entity = None
                new_entity = re.search(r'\/class\/Journal\>', decoded_line)
                if new_entity is not None:
                    if entity is not None:
                        writer.writerow([entity, rank, name, issn, paper_count, citation_count, creation_date])

                    entity = None
                    rank = None
                    name = None
                    issn = None
                    paper_count = None
                    citation_count = None
                    creation_date = None

                # parse entity
                entity_url = None
                entity_url = re.search(r'^\<http\:\/\/ma\-graph\.org\/entity\/[0-9]*', decoded_line)
                entity_url = str(entity_url.group(0))
                entity = entity_url.rsplit('/', 1)[1]

                # parse rank
                rank_presence = None
                rank_presence = re.search(r'\/property\/rank\> ', decoded_line)
                if rank_presence is not None:
                    rank_line = re.search(r'"[0-9]*"', decoded_line)
                    rank = str(rank_line.group(0))
                    rank = rank.split('"', 1)[1]
                    rank = rank.split('"', 1)[0]

                # parse name
                name_presence = None
                name_presence = re.search(r'\/name\> ', decoded_line)
                if name_presence is not None:
                    name_line = re.search(r'.*', decoded_line)
                    name = str(name_line.group(0))
                    name = name.split('"', 1)[1]
                    name = name.split('"', 1)[0]

                # parse issn
                issn_presence = None
                issn_presence = re.search(r'\/identifiers\/issn> ',
                                          decoded_line)
                if issn_presence is not None:
                    issn_line = re.search(r'"([0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}|[0-9a-zA-Z]{8})"', decoded_line)
                    issn = str(issn_line.group(0))
                    issn = issn.split('"', 1)[1]
                    issn = issn.split('"', 1)[0]

                # parse paper_count
                paper_count_presence = None
                paper_count_presence = re.search(r'\/property\/paperCount\> ',
                                                 decoded_line)
                if paper_count_presence is not None:
                    paper_count_line = re.search(r'"[0-9]+"', decoded_line)
                    paper_count = str(paper_count_line.group(0))
                    paper_count = paper_count.split('"', 1)[1]
                    paper_count = paper_count.split('"', 1)[0]

                # parse citation_count
                citation_count_presence = None
                citation_count_presence = re.search(r'\/property\/citationCount\> ',
                                                    decoded_line)
                if citation_count_presence is not None:
                    citation_count_line = re.search(r'"[0-9]+"', decoded_line)
                    citation_count = str(citation_count_line.group(0))
                    citation_count = citation_count.split('"', 1)[1]
                    citation_count = citation_count.split('"', 1)[0]

                # parse creation_date
                creation_date_presence = None
                creation_date_presence = re.search(r'\/dc\/terms\/created\> ', decoded_line)
                if creation_date_presence is not None:
                    creation_date_line = re.search(r'[0-9]{4}\-(0|1)[0-9]\-[0-3][0-9]', decoded_line)
                    creation_date = str(creation_date_line.group(0))


def parse_fields_of_study(file_name):

    entity = None
    rank = None
    name = None
    category = None
    level = None
    paper_count = None
    citation_count = None
    creation_date = None

    with open('../data/original/fieldsOfStudy.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['entity', 'rank', 'name', 'category', 'level', 'paper_count', 'citation_count', 'created'])

        with bzopen(file_name, "r") as file:
            for line in file:
                decoded_line = line.decode("utf-8")

                new_entity = None
                new_entity = re.search(r'\/class\/FieldOfStudy\>', decoded_line)
                if new_entity is not None:
                    if entity is not None:
                        writer.writerow(
                            [entity, rank, name, category, level, paper_count, citation_count, creation_date])

                    entity = None
                    rank = None
                    name = None
                    category = None
                    level = None
                    paper_count = None
                    citation_count = None
                    creation_date = None

                # parse entity
                entity_url = None
                entity_url = re.search(r'^\<http\:\/\/ma\-graph\.org\/entity\/[0-9]*', decoded_line)
                entity_url = str(entity_url.group(0))
                entity = entity_url.rsplit('/', 1)[1]

                # parse rank
                rank_presence = None
                rank_presence = re.search(r'.\/property\/rank\> ', decoded_line)
                if rank_presence is not None:
                    rank_line = re.search(r'"[0-9]*"', decoded_line)
                    rank = str(rank_line.group(0))
                    rank = rank.split('"', 1)[1]
                    rank = rank.split('"', 1)[0]

                # parse name
                name_presence = None
                name_presence = re.search(r'\/name\> ', decoded_line)
                if name_presence is not None:
                    name_line = re.search(r'.*', decoded_line)
                    name = str(name_line.group(0))
                    name = name.split('"', 1)[1]
                    name = name.split('"', 1)[0]

                # parse category
                category_presence = None
                category_presence = re.search(r'\/property\/category\> ', decoded_line)
                if category_presence is not None:
                    category_line = re.search(r'.*', decoded_line)
                    category = str(category_line.group(0))
                    category = category.split('"', 1)[1]
                    category = category.split('"', 1)[0]

                # parse level
                level_presence = None
                level_presence = re.search(r'\/property\/level\> ', decoded_line)
                if level_presence is not None:
                    level_line = re.search(r'"[0-9]+"', decoded_line)
                    level = str(level_line.group(0))
                    level = level.split('"', 1)[1]
                    level = level.split('"', 1)[0]

                # parse paper_count
                paper_count_presence = None
                paper_count_presence = re.search(r'\/property\/paperCount\> ', decoded_line)
                if paper_count_presence is not None:
                    paper_count_line = re.search(r'"[0-9]+"', decoded_line)
                    paper_count = str(paper_count_line.group(0))
                    paper_count = paper_count.split('"', 1)[1]
                    paper_count = paper_count.split('"', 1)[0]

                # parse citation_count
                citation_count_presence = None
                citation_count_presence = re.search(r'\/property\/citationCount\> ', decoded_line)
                if citation_count_presence is not None:
                    citation_count_line = re.search(r'"[0-9]+"', decoded_line)
                    citation_count = str(citation_count_line.group(0))
                    citation_count = citation_count.split('"', 1)[1]
                    citation_count = citation_count.split('"', 1)[0]

                # parse creation_date
                creation_date_presence = None
                creation_date_presence = re.search(r'\/dc\/terms\/created\> ', decoded_line)
                if creation_date_presence is not None:
                    creation_date_line = re.search(r'[0-9]{4}\-(0|1)[0-9]\-[0-3][0-9]', decoded_line)
                    creation_date = str(creation_date_line.group(0))


def parse_paper_languages(file_name):

    language = None
    paper = None

    with open('../data/original/paperLanguages.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['paper', 'language'])

        with bzopen(file_name, "r") as file:
            for line in file:
                decoded_line = line.decode("utf-8")

                # parse paper
                paper_url = None
                paper = None
                paper_url = re.search(r'^\<http\:\/\/ma\-graph\.org\/entity\/[0-9]*', decoded_line)
                paper_url = str(paper_url.group(0))
                paper = paper_url.rsplit('/', 1)[1]

                # parse language
                language_url = None
                language = None
                language_url = re.search(r'"[a-z]{2}(\_)?[a-z]*"', decoded_line)
                language = str(language_url.group(0))
                language = language.split('"', 1)[1]
                language = language.split('"', 1)[0]

                writer.writerow([paper, language])


def parse_paper_authors(file_name):

    author = None
    paper = None

    with open('../data/original/paperAuthors.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['paper', 'author'])

        with bzopen(file_name, "r") as file:
            for line in file:
                decoded_line = line.decode("utf-8")

                # parse paper
                paper_url = None
                paper = None
                paper_url = re.search(r'^\<http\:\/\/ma\-graph\.org\/entity\/[0-9]*', decoded_line)
                paper_url = str(paper_url.group(0))
                paper = paper_url.rsplit('/', 1)[1]

                # parse author
                author_url = None
                author = None
                author_url = re.search(r' \<http\:\/\/ma\-graph\.org\/entity\/[0-9]*\> \.', decoded_line)
                author_url = str(author_url.group(0))
                author = author_url.rsplit('/', 1)[1]
                author = author[:-3]

                writer.writerow([paper, author])


def parse_paper_fields(file_name):

    field_of_study = None
    paper = None

    with open('../data/original/paperFieldsOfStudy.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['paper', 'author'])

        with bzopen(file_name, "r") as file:
            for line in file:
                decoded_line = line.decode("utf-8")

                # parse paper
                paper_url = None
                paper = None
                paper_url = re.search(r'^\<http\:\/\/ma\-graph\.org\/entity\/[0-9]*\>', decoded_line)
                paper_url = str(paper_url.group(0))
                paper = paper_url.rsplit('/', 1)[1]
                paper = paper[:-1]

                # parse field_of_study
                field_of_study_url = None
                field_of_study = None
                field_of_study_url = re.search(r' \<http\:\/\/ma\-graph\.org\/entity\/[0-9]*\> \.', decoded_line)
                field_of_study_url = str(field_of_study_url.group(0))
                field_of_study = field_of_study_url.rsplit('/', 1)[1]
                field_of_study = field_of_study[:-3]

                writer.writerow([paper, field_of_study])


def parse_children(file_name):

    child = None
    parent = None

    with open('../data/original/fieldsOfStudyChildren.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['child', 'parent'])

        with bzopen(file_name, "r") as file:
            for line in file:
                decoded_line = line.decode("utf-8")

                # parse child
                child_url = None
                child = None
                child_url = re.search(r'^\<http\:\/\/ma\-graph\.org\/entity\/[0-9]*\>', decoded_line)
                child_url = str(child_url.group(0))
                child = child_url.rsplit('/', 1)[1]
                child = child[:-1]

                # parse parent
                parent_url = None
                parent = None
                parent_url = re.search(r' \<http\:\/\/ma\-graph\.org\/entity\/[0-9]*\> \.', decoded_line)
                parent_url = str(parent_url.group(0))
                parent = parent_url.rsplit('/', 1)[1]
                parent = parent[:-3]

                writer.writerow([child, parent])


def parse_papers(file_name):

    entity = None
    rank = None
    datacite = None
    title = None
    publisher = None
    volume = None
    issue_identifier = None
    starting_page = None
    ending_page = None
    appears_in_journal = None
    reference_count = None
    citation_count = None
    creation_date = None

    with open('../data/original/papers.csv', 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(
            ['entity', 'rank', 'datacite', 'title', 'publisher', 'volume', 'issue_identifier', 'starting_page',
             'ending_page', 'appears_in_journal', 'reference_count', 'citation_count', 'created'])

        with bzopen(file_name, "r") as file:
            for line in file:
                decoded_line = line.decode("utf-8")

                new_entity = None
                new_entity = re.search(r'\/class\/Paper\>', decoded_line)
                if new_entity is not None:
                    if entity is not None:
                        writer.writerow(
                            [entity, rank, datacite, title, publisher, volume, issue_identifier, starting_page, ending_page,
                             appears_in_journal, reference_count, citation_count, creation_date])
                        # print(entity, rank, datacite, title, publisher, volume, issue_identifier, starting_page,
                        #       ending_page, appears_in_journal, reference_count, citation_count, creation_date)

                        entity = None
                        rank = None
                        datacite = None
                        title = None
                        publisher = None
                        volume = None
                        issue_identifier = None
                        starting_page = None
                        ending_page = None
                        appears_in_journal = None
                        reference_count = None
                        citation_count = None
                        creation_date = None

                # parse entity
                entity_url = None
                entity_url = re.search(r'^\<http\:\/\/ma\-graph\.org\/entity\/[0-9]*', decoded_line)
                if entity_url is not None:
                    entity_url = str(entity_url.group(0))
                    entity = entity_url.rsplit('/', 1)[1]

                # parse rank
                rank_presence = None
                rank_presence = re.search(r'.\/property\/rank\> ', decoded_line)
                if rank_presence is not None:
                    rank_line = re.search(r'"[0-9]*"', decoded_line)
                    if rank_line is not None:
                        rank = str(rank_line.group(0))
                        rank = rank.split('"', 1)[1]
                        rank = rank.split('"', 1)[0]

                # parse datacite
                datacite_presence = None
                datacite_presence = re.search(r'\/spar\/datacite\/doi\>', decoded_line)
                if datacite_presence is not None:
                    datacite_line = re.search(r'".*"', decoded_line)
                    if datacite_line is not None:
                        datacite = str(datacite_line.group(0))
                        datacite = datacite.split('"', 1)[1]
                        datacite = datacite.split('"', 1)[0]

                # parse title
                title_presence = None
                title_presence = re.search(r'\/dc\/terms\/title\> ', decoded_line)
                if title_presence is not None:
                    title_line = re.search(r' "(.+?)"\^\^', decoded_line)
                    if title_line is not None:
                        title = str(title_line.group(0))
                        title = title.split('"', 1)[1]
                        title = title.split('"', 1)[0]

                # parse publisher
                publisher_presence = None
                publisher_presence = re.search(r'\/dc\/terms\/publisher\> ', decoded_line)
                if publisher_presence is not None:
                    publisher_line = re.search(r'.*', decoded_line)
                    if publisher_line is not None:
                        publisher = str(publisher_line.group(0))
                        publisher = publisher.split('"', 1)[1]
                        publisher = publisher.split('"', 1)[0]

                # parse volume
                volume_presence = None
                volume_presence = re.search(r'\/volume\> ', decoded_line)
                if volume_presence is not None:
                    volume_line = re.search(r'"[0-9]+"', decoded_line)
                    if volume_line is not None:
                        volume = str(volume_line.group(0))
                        volume = volume.split('"', 1)[1]
                        volume = volume.split('"', 1)[0]

                # parse issue_identifier
                issue_identifier_presence = None
                issue_identifier_presence = re.search(r'\/issueIdentifier\> ', decoded_line)
                if issue_identifier_presence is not None:
                    issue_identifier_line = re.search(r'"[0-9]+"', decoded_line)
                    if issue_identifier_line is not None:
                        issue_identifier = str(issue_identifier_line.group(0))
                        issue_identifier = issue_identifier.split('"', 1)[1]
                        issue_identifier = issue_identifier.split('"', 1)[0]

                # parse starting_page
                starting_page_presence = None
                starting_page_presence = re.search(r'\/startingPage> ', decoded_line)
                if starting_page_presence is not None:
                    starting_page_line = re.search(r'"[0-9]+"', decoded_line)
                    if starting_page_line is not None:
                        starting_page = str(starting_page_line.group(0))
                        starting_page = starting_page.split('"', 1)[1]
                        starting_page = starting_page.split('"', 1)[0]

                # parse ending_page
                ending_page_presence = None
                ending_page_presence = re.search(r'\/endingPage\> ', decoded_line)
                if ending_page_presence is not None:
                    ending_page_line = re.search(r'"[0-9]+"', decoded_line)
                    if ending_page_line is not None:
                        ending_page = str(ending_page_line.group(0))
                        ending_page = ending_page.split('"', 1)[1]
                        ending_page = ending_page.split('"', 1)[0]

                # parse reference_count
                reference_count_presence = None
                reference_count_presence = re.search(r'\/property\/referenceCount\> ', decoded_line)
                if reference_count_presence is not None:
                    reference_count_line = re.search(r'"[0-9]+"', decoded_line)
                    if reference_count_line is not None:
                        reference_count = str(reference_count_line.group(0))
                        reference_count = reference_count.split('"', 1)[1]
                        reference_count = reference_count.split('"', 1)[0]

                # parse citation_count
                citation_count_presence = None
                citation_count_presence = re.search(r'\/property\/citationCount\> ', decoded_line)
                if citation_count_presence is not None:
                    citation_count_line = re.search(r'"[0-9]+"', decoded_line)
                    if citation_count_line is not None:
                        citation_count = str(citation_count_line.group(0))
                        citation_count = citation_count.split('"', 1)[1]
                        citation_count = citation_count.split('"', 1)[0]

                # parse creation_date
                creation_date_presence = None
                creation_date_presence = re.search(r'\/dc\/terms\/created\> ', decoded_line)
                if creation_date_presence is not None:
                    creation_date_line = re.search(r'[0-9]{4}\-(0|1)[0-9]\-[0-3][0-9]', decoded_line)
                    creation_date = str(creation_date_line.group(0))


def parse_all_files():
    print('------- Start parsing files -------')
    parse_affiliations("../data/original/zip/Affiliations.nt.bz2")
    parse_authors("../data/original/zip/Authors.nt.bz2")
    parse_conference_instances("../data/original/zip/ConferenceInstances.nt.bz2")
    parse_conference_series("../data/original/zip/ConferenceSeries.nt.bz2")
    parse_journals("../data/original/zip/Journals.nt.bz2")
    parse_fields_of_study("../data/original/zip/FieldsOfStudy.nt.bz2")
    parse_paper_languages("../data/original/zip/PaperLanguages.nt.bz2")
    parse_paper_authors("../data/original/zip/PaperAuthorAffiliations.nt.bz2")
    parse_paper_fields("../data/original/zip/PaperFieldsOfStudy.nt.bz2")
    parse_papers("../data/original/zip/Papers.nt.bz2")
    print('------- THE END -------')


if __name__ == "__main__":
    parse_all_files()
