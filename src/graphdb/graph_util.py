from py2neo import Graph
from tqdm import tqdm

def init_db(delete = False):
    """ initializes the db connection and returns the graph object.
    TODO: Enable variables
    """
    uri = "bolt://localhost:7687"
    print("Connecting to database at %s.." % uri)
    graph = Graph(uri, auth=("neo4j", "admin"))
    print("Connected.")
    if delete:
        print("Deleting graph at %s.." % uri)
        graph.delete_all()
        print("Deleted")
    return graph


def query_label_to_dict(graph, label, attribute):
    label_dict = {}
    temp = graph.nodes.match(label)
    for node in temp:
        label_dict[node[attribute]] = node
    return label_dict

def create_in_batch(graph, objects):
    batches = [objects[i:i + 5000] for i in range(0, len(objects), 5000)]
    with tqdm(total=len(objects)) as pbar:
        for batch in batches:
            tx = graph.begin()
            for object in batch:
                tx.create(object)
                pbar.update(1)
            tx.commit()
