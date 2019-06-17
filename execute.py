import util as ut

path='KG'
bound=1000
sentences = ut.generator_of_reader(ut.get_path_knowledge_graphs(path))

for s,p,o in sentences:
    print(s,p,o)