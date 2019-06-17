import re
from typing import Sequence
import os
import bz2


def parse_n3(sentence):
    flag = 0

    components = re.findall('<(.+?)>', sentence)

    if len(components) == 2:
        # N3 contains a literal.
        s, p = components
        remaining_sentence = sentence[sentence.index(p) + len(p) + 2:]
        literal = remaining_sentence[:-2]
        o = literal
        flag = 2

    elif len(components) == 3:
        # N3 consists of URIs.
        s, p, o = components
        flag = 3


    elif len(components) > 3:
        raise NotImplemented()

    else:
        ## This means that literal contained in RDF triple contains < > symbol
        print('Sentence:{0}\tSentence length:{1}'.format(sentence.encode(), len(sentence)))
        print('Extracted fields:{0}\tExtracted fields length:{1}'.format(components,len(components)))
        raise ValueError()

    s = re.sub("\s+", "", s)
    p = re.sub("\s+", "", p)
    o = re.sub("\s+", "", o)

    return s, p, o, flag


def get_path_knowledge_graphs(path: str) -> Sequence[str]:
    """

    :param path: str represents path of a KG or path of folder containing KGs
    :return: Sequence of paths
    """
    KGs = list()

    if os.path.isfile(path):
        KGs.append(path)
    else:
        for root, dir, files in os.walk(path):
            for file in files:
                if '.nq' in file or '.nt' in file or 'ttl' in file:
                    KGs.append(path + '/' + file)
    if len(KGs) == 0:
        print(path + ' is not a path for a file or a folder containing any .nq or .nt formatted files')
        exit(1)
    return KGs


def getreader(f_name):
    if f_name[-4:] == '.bz2':
        reader = bz2.open(f_name, "rt")
        return reader
    return open(f_name, "r")


def generator_of_reader(knowledge_graphs, rdf_parser=parse_n3, bound=None):
    """

    :param bound:
    :param knowledge_graphs:
    :param rdf_parser:
    :return:
    """
    if bound is None:
        for f_name in knowledge_graphs:
            reader = getreader(f_name)
            for sentence in reader:
                if len(sentence)>1:

                    try:
                        s, p, o, flag = rdf_parser(sentence)
                        yield s, p, o
                    except ValueError:
                        raise ValueError()


    else:

        for f_name in knowledge_graphs:
            reader = getreader(f_name)
            total_sentence = 0
            for sentence in reader:

                if total_sentence == bound: break
                total_sentence += 1

                try:
                    s, p, o, flag = rdf_parser(sentence)

                except ValueError:
                    raise ValueError()

                yield s, p, o



###########some codes
## get all indexes
#To get all indices es.indices.get_alias("*")

# es.search(index="rdf")

# To do upsirt
#https://stackoverflow.com/questions/33226831/how-to-use-python-elasticsearch-client-upsert-api