from neo4j import GraphDatabase, basic_auth
from neo4j.graph import Node, Relationship
from typing import List, Dict, Any, Optional
import secrets
import string
import os
from dotenv import load_dotenv
from pprint import pprint

# Загружаем переменные окружения из .env
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
TNode = Dict[str, Any]
TArc = Dict[str, Any]

class Neo4jRepository:
    def __init__(self,
                 uri=NEO4J_URI,
                 user=NEO4J_USER,
                 password=NEO4J_PASSWORD):
        self.driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))

    def close(self):
        self.driver.close()

    @staticmethod
    def generate_random_string(length: int = 12) -> str:
        alphabet = string.ascii_letters + string.digits
        return ''.join(secrets.choice(alphabet) for _ in range(length))

    # ---------- исправленные методы ----------
    def get_all_nodes(self) -> List[Dict[str, Any]]:
        """
        Возвращает все узлы.
        """
        query = "MATCH (n) RETURN n"
        return self.run_custom_query(query)

    def get_all_nodes_and_arcs(self) -> List[Dict[str, Any]]:
        """
        Возвращает все узлы и их исходящие рёбра.
        """
        out = []
        with self.driver.session() as s:
            # Узлы
            res_nodes = s.run("MATCH (n) RETURN n")
            nodes_map = {}
            for r in res_nodes:
                n = r["n"]
                node = {
                    "_type": "node",
                    "id": getattr(n, "element_id", ""),
                    "properties": dict(n),
                    "arcs": []
                }
                nodes_map[node["id"]] = node

            # Рёбра
            res_arcs = s.run("MATCH (a)-[r]->(b) RETURN a, r, b")
            for r in res_arcs:
                rel = r["r"]
                arc = {
                    "_type": "rel",
                    "id": getattr(rel, "element_id", ""),
                    "type": getattr(rel, "type", ""),
                    "properties": dict(rel),
                    "start": getattr(rel.start_node, "element_id", ""),
                    "end": getattr(rel.end_node, "element_id", "")
                }
                start_id = arc["start"]
                if start_id in nodes_map:
                    nodes_map[start_id]["arcs"].append(arc)

            out = list(nodes_map.values())
        return out

    def get_nodes_by_labels(self, labels: List[str]) -> List[Dict[str, Any]]:
        if not labels:
            return []
        label_expr = ":" + ":".join([lbl.replace(":", "") for lbl in labels])
        query = f"MATCH (n{label_expr}) RETURN n"
        return self.run_custom_query(query)

    def get_node_by_uri(self, uri: str) -> Optional[Dict[str, Any]]:
        query = "MATCH (n {uri: $uri}) RETURN n LIMIT 1"
        rows = self.run_custom_query(query, {"uri": uri})
        return rows[0]["n"] if rows else None

    def create_node(self, params: Dict[str, Any], labels: Optional[List[str]] = None) -> Dict[str, Any]:
        props = dict(params)
        if "uri" not in props or not props["uri"]:
            props["uri"] = self.generate_random_string(12)

        label_str = ""
        if labels:
            label_str = ":" + ":".join([lbl.replace(":", "") for lbl in labels])

        query = f"CREATE (n{label_str} $props) RETURN n"
        rows = self.run_custom_query(query, {"props": props})

        node = rows[0]["n"]
        # прокинем uri наружу для удобства
        node["uri"] = node["properties"].get("uri", "")

        return node

    def create_arc(self,
                   node1_uri: str,
                   node2_uri: str,
                   rel_type: str = "RELATED",
                   rel_props: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        rel_props = rel_props or {}
        query = f"""
            MATCH (a {{uri: $u1}}), (b {{uri: $u2}})
            CREATE (a)-[r:{rel_type} $rprops]->(b)
            RETURN r
        """
        rows = self.run_custom_query(query, {"u1": node1_uri, "u2": node2_uri, "rprops": rel_props})
        return rows[0]["r"] if rows else None

    def delete_node_by_uri(self, uri: str, detach: bool = True) -> int:
        if detach:
            query = "MATCH (n {uri: $uri}) DETACH DELETE n RETURN count(n) AS cnt"
        else:
            query = "MATCH (n {uri: $uri}) DELETE n RETURN count(n) AS cnt"
        rows = self.run_custom_query(query, {"uri": uri})
        return int(rows[0]["cnt"]) if rows else 0

    def delete_arc_by_id(self, arc_element_id: str) -> bool:
        query = "MATCH ()-[r]-() WHERE elementId(r) = $rid DELETE r RETURN count(r) AS cnt"
        rows = self.run_custom_query(query, {"rid": arc_element_id})
        return rows and int(rows[0]["cnt"]) > 0

    def update_node(self, uri: str, properties: Dict[str, Any], merge: bool = False) -> Optional[Dict[str, Any]]:
        if merge:
            query = "MATCH (n {uri: $uri}) SET n += $properties RETURN n"
        else:
            query = "MATCH (n {uri: $uri}) SET n = $properties RETURN n"
        rows = self.run_custom_query(query, {"uri": uri, "properties": properties})
        return rows[0]["n"] if rows else None

    def run_custom_query(self, query: str, parameters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Выполняет произвольный Cypher-запрос. Узлы и рёбра сериализуются автоматически.
        """
        parameters = parameters or {}
        with self.driver.session() as s:
            res = s.run(query, **parameters)
            out = []
            for record in res:
                rec = {}
                for k in record.keys():
                    v = record[k]
                    if isinstance(v, Node):
                        rec[k] = {
                            "_type": "node",
                            "id": getattr(v, "element_id", ""),
                            "properties": dict(v)
                        }
                        continue
                    if isinstance(v, Relationship):
                        rec[k] = {
                            "_type": "rel",
                            "id": getattr(v, "element_id", ""),
                            "type": getattr(v, "type", ""),
                            "properties": dict(v),
                            "start": getattr(v.start_node, "element_id", ""),
                            "end": getattr(v.end_node, "element_id", "")
                        }
                        continue
                    rec[k] = v
                out.append(rec)
            return out

# ---- Пример использования ----
if __name__ == "__main__":
    repo = Neo4jRepository()
    try:
        # создать узлы
        n1 = repo.create_node({"title": "Node A", "description": "Первый узел"})
        n2 = repo.create_node({"title": "Node B", "description": "Второй узел"})
        print("Created nodes:", n1, n2)

        # создать ребро (используем uri автоматически)
        arc = repo.create_arc(n1["uri"], n2["uri"], rel_type="LINKS")
        print("Created arc:", arc)

        # получить все узлы и рёбра
        all_with_arcs = repo.get_all_nodes_and_arcs()
        print("All nodes with arcs:", all_with_arcs)

        # выполнить произвольный запрос
        custom = repo.run_custom_query("MATCH (a)-[r]->(b) RETURN a, r, b LIMIT 5")
        print("Custom query result sample:", custom[:1])

        repo.run_custom_query("MATCH (p) DETACH DELETE p ")

    finally:
        repo.close()
