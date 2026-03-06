import polars as pl
import networkx as nx


TECH_RELATIONS = {
    "llm": ["langchain", "rag", "vector database"],
    "machine learning": ["mlops", "feature store"],
    "generative ai": ["llm", "prompt engineering"],
    "data engineering": ["mlops", "feature store"]
}


def build_trend_graph():

    G = nx.Graph()

    for parent, children in TECH_RELATIONS.items():

        G.add_node(parent)

        for child in children:

            G.add_node(child)

            G.add_edge(parent, child)

    return G


def graph_to_dataframe(G):

    edges = []

    for source, target in G.edges():

        edges.append({
            "source": source,
            "target": target
        })

    return pl.DataFrame(edges)


def show_graph_info(G):

    print("\n🧠 AI Trend Graph\n")

    print("Nodes:", G.number_of_nodes())

    print("Connections:", G.number_of_edges())


if __name__ == "__main__":

    graph = build_trend_graph()

    show_graph_info(graph)

    df = graph_to_dataframe(graph)

    print("\nConnections:\n")

    print(df)