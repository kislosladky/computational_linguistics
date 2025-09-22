from neo4j import GraphDatabase, basic_auth
from neo4j.graph import Node, Relationship
from typing import List, Dict, Any, Optional
import secrets
import string
import os
from dotenv import load_dotenv

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
        """
        Создаёт подключение к Neo4j.
        """
        self.driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))

    def close(self):
        self.driver.close()

    @staticmethod
    def collect_node(record_node: Any) -> TNode:
        """
        Универсальная сериализация узла в TNode.
        Принимает:
          - neo4j.graph.Node,
          - или dict вида {"id": ..., "properties": {...}}
          - или запись из запроса с полями id, properties
        Возвращаемое id — строка (elementId).
        """
        node_id = None
        props = {}

        # neo4j.graph.Node
        try:

            if isinstance(record_node, Node):
                node_id = getattr(record_node, "element_id", None)
                props = dict(record_node)
        except Exception:
            pass

        if node_id is None and isinstance(record_node, dict):
            node_id = record_node.get("id") or record_node.get("elementId") or record_node.get("element_id")
            properties = record_node.get("properties")
            if isinstance(properties, dict):
                props = properties
            else:
                props = record_node.get("node") if isinstance(record_node.get("node"), dict) else props

        if node_id is None and hasattr(record_node, "get"):
            node_id = record_node.get("id") or node_id
            if "properties" in record_node:
                props = record_node.get("properties") or props

        node_id = str(node_id) if node_id is not None else ""
        uri = props.get("uri", "") or props.get("Uri", "") or props.get("URI", "")
        title = props.get("title", "") or ""
        description = props.get("description", "") or ""

        return {
            "id": node_id,
            "uri": str(uri),
            "title": str(title),
            "description": str(description),
        }

    @staticmethod
    def collect_arc(record_arc: Any) -> TArc:
        """
        Универсальная сериализация ребра в TArc.
        Принимает neo4j Relationship или словарь с ключами rid/type/properties/start/end.
        id возвращается как строка elementId.
        """
        rel_id = None
        props = {}
        r_type = ""
        start_elem = ""
        end_elem = ""

        try:
            if isinstance(record_arc, Relationship):
                rel_id = getattr(record_arc, "element_id", None)
                props = dict(record_arc)
                r_type = getattr(record_arc, "type", "") or ""

                start_elem = getattr(record_arc.start_node, "element_id", "") or ""
                end_elem = getattr(record_arc.end_node, "element_id", "") or ""
        except Exception:
            pass

        if rel_id is None and isinstance(record_arc, dict):
            rel_id = record_arc.get("rid") or record_arc.get("id") or record_arc.get("elementId")
            props = record_arc.get("r_props") or record_arc.get("properties") or {}
            r_type = record_arc.get("r_type") or record_arc.get("type") or r_type
            start_elem = record_arc.get("a_uri") or record_arc.get("start") or start_elem
            end_elem = record_arc.get("b_uri") or record_arc.get("end") or end_elem

        arc_uri = props.get("uri") or r_type or ""
        node_uri_from = props.get("node_uri_from") or props.get("from") or start_elem or ""
        node_uri_to = props.get("node_uri_to") or props.get("to") or end_elem or ""

        rel_id = str(rel_id) if rel_id is not None else ""

        return {
            "id": rel_id,
            "uri": str(arc_uri),
            "node_uri_from": str(node_uri_from),
            "node_uri_to": str(node_uri_to)
        }


    @staticmethod
    def generate_random_string(length: int = 12) -> str:
        alphabet = string.ascii_letters + string.digits
        return ''.join(secrets.choice(alphabet) for _ in range(length))


    def get_all_nodes(self) -> List[TNode]:
        """
        Возвращает список всех узлов (без связей).
        Использует elementId(n) для получения идентификатора (строка).
        """
        query = "MATCH (n) RETURN elementId(n) AS id, properties(n) AS properties"
        with self.driver.session() as s:
            res = s.run(query)
            nodes = []
            for record in res:
                rec = {"id": record["id"], "properties": record["properties"]}
                nodes.append(self.collect_node(rec))
            return nodes


    def get_all_nodes_and_arcs(self) -> List[TNode]:
        """
        Возвращает все узлы и в поле 'arcs' — список исходящих рёбер (TArc).
        Использует elementId(...) в возвращаемых полях.
        """
        with self.driver.session() as s:
            nodes_map: Dict[str, TNode] = {}
            res_nodes = s.run("MATCH (n) RETURN elementId(n) AS id, properties(n) AS properties")
            for r in res_nodes:
                node = self.collect_node({"id": r["id"], "properties": r["properties"]})
                node["arcs"] = []
                nodes_map[node["id"]] = node

            res_arcs = s.run("""
                MATCH (a)-[r]->(b)
                RETURN elementId(r) AS rid, type(r) AS r_type,
                       properties(r) AS r_props,
                       elementId(a) AS aid, properties(a).uri AS a_uri,
                       elementId(b) AS bid, properties(b).uri AS b_uri
            """)
            for r in res_arcs:
                props = dict(r["r_props"]) if r["r_props"] is not None else {}
                arc = {
                    "rid": r["rid"],
                    "r_props": props,
                    "r_type": r["r_type"],
                    "a_id": r["aid"],
                    "a_uri": r["a_uri"],
                    "b_id": r["bid"],
                    "b_uri": r["b_uri"]
                }
                arc_obj = self.collect_arc({
                    "rid": arc["rid"],
                    "r_props": arc["r_props"],
                    "r_type": arc["r_type"],
                    "a_uri": arc["a_uri"],
                    "b_uri": arc["b_uri"],
                    "start": arc["a_id"],
                    "end": arc["b_id"]
                })
                src_id = str(arc["a_id"]) if arc["a_id"] is not None else ""
                if src_id and src_id in nodes_map:
                    nodes_map[src_id]["arcs"].append(arc_obj)
                else:
                    pass

            return list(nodes_map.values())

    def get_nodes_by_labels(self, labels: List[str]) -> List[TNode]:
        """
        Возвращает узлы, у которых есть все указанные метки (AND).
        """
        if not labels:
            return []
        label_expr = ":" + ":".join([lbl.replace(":", "") for lbl in labels])
        query = f"MATCH (n{label_expr}) RETURN elementId(n) AS id, properties(n) AS properties"
        with self.driver.session() as s:
            res = s.run(query)
            nodes = [ self.collect_node({"id": r["id"], "properties": r["properties"]}) for r in res ]
            return nodes

    def get_node_by_uri(self, uri: str) -> Optional[TNode]:
        """
        Находит узел по свойству uri.
        Возвращает TNode или None.
        """
        query = "MATCH (n {uri: $uri}) RETURN elementId(n) AS id, properties(n) AS properties LIMIT 1"
        with self.driver.session() as s:
            r = s.run(query, uri=uri).single()
            if r is None:
                return None
            node = self.collect_node({"id": r["id"], "properties": r["properties"]})
            return node

    def create_node(self, params: Dict[str, Any], labels: Optional[List[str]] = None) -> TNode:
        """
        Создаёт узел. Если params не содержит 'uri' — создаёт его.
        Возвращает TNode с elementId в поле id.
        """
        props = dict(params)
        if "uri" not in props or not props["uri"]:
            props["uri"] = self.generate_random_string(12)

        label_str = ""
        if labels:
            label_str = ":" + ":".join([lbl.replace(":", "") for lbl in labels])

        query = f"CREATE (n{label_str} $props) RETURN elementId(n) AS id, properties(n) AS properties"
        with self.driver.session() as s:
            r = s.run(query, props=props).single()
            return self.collect_node({"id": r["id"], "properties": r["properties"]})

    def create_arc(self, node1_uri: str, node2_uri: str, rel_type: str = "RELATED", rel_props: Optional[Dict[str, Any]] = None) -> Optional[TArc]:
        """
        Создаёт ребро между узлами с uri=node1_uri и uri=node2_uri.
        Возвращает TArc (id как elementId) или None если узлы не найдены.
        """
        rel_props = rel_props or {}
        query = f"""
            MATCH (a {{uri: $u1}}), (b {{uri: $u2}})
            CREATE (a)-[r:{rel_type} $rprops]->(b)
            RETURN elementId(r) AS rid, type(r) AS r_type, properties(r) AS r_props,
                   properties(a).uri AS a_uri, properties(b).uri AS b_uri,
                   elementId(a) AS a_id, elementId(b) AS b_id
        """
        with self.driver.session() as s:
            r = s.run(query, u1=node1_uri, u2=node2_uri, rprops=rel_props).single()
            if r is None:
                return None
            props = dict(r["r_props"]) if r["r_props"] is not None else {}
            arc = {
                "id": str(r["rid"]),
                "uri": props.get("uri") or r["r_type"] or "",
                "node_uri_from": r["a_uri"] or str(r["a_id"]) or "",
                "node_uri_to": r["b_uri"] or str(r["b_id"]) or ""
            }
            return arc

    def delete_node_by_uri(self, uri: str, detach: bool = True) -> int:
        """
        Удаляет узел по uri. Возвращает количество удалённых узлов (0 или 1).
        """
        if detach:
            query = "MATCH (n {uri: $uri}) DETACH DELETE n RETURN count(*) AS cnt"
        else:
            query = "MATCH (n {uri: $uri}) DELETE n RETURN count(*) AS cnt"
        with self.driver.session() as s:
            r = s.run(query, uri=uri).single()
            return int(r["cnt"]) if r and r["cnt"] is not None else 0

    def delete_arc_by_id(self, arc_element_id: str) -> bool:
        """
        Удаляет ребро по elementId (строке).
        Возвращает True если удалено, False если не найдено.
        """
        query = "MATCH ()-[r]-() WHERE elementId(r) = $rid DELETE r RETURN count(*) AS cnt"
        with self.driver.session() as s:
            r = s.run(query, rid=arc_element_id).single()
            cnt = int(r["cnt"]) if r and r["cnt"] is not None else 0
            return cnt > 0

    def update_node(self, uri: str, properties: Dict[str, Any], merge: bool = False) -> Optional[TNode]:
        """
        Обновляет свойства узла с данным uri.
        Если merge=True — merge свойства (обновляет/добавляет), иначе перезаписывает свойства (SET n = $properties).
        """
        if merge:
            query = "MATCH (n {uri: $uri}) SET n += $properties RETURN elementId(n) AS id, properties(n) AS properties"
        else:
            query = "MATCH (n {uri: $uri}) SET n = $properties RETURN elementId(n) AS id, properties(n) AS properties"
        with self.driver.session() as s:
            r = s.run(query, uri=uri, properties=properties).single()
            if r is None:
                return None
            return self.collect_node({"id": r["id"], "properties": r["properties"]})

    def run_custom_query(self, query: str, parameters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Выполняет произвольный Cypher-запрос. Сериализует Node/Relationship в понятную форму.
        """
        parameters = parameters or {}
        with self.driver.session() as s:
            res = s.run(query, **parameters)
            out = []
            for record in res:
                rec = {}
                for k in record.keys():
                    v = record[k]
                    # try detect Node/Relationship using attributes
                    try:
                        if isinstance(v, Node):
                            rec[k] = {
                                "_type": "node",
                                "id": getattr(v, "element_id", "") or "",
                                "properties": dict(v)
                            }
                            continue
                        if isinstance(v, Relationship):
                            rec[k] = {
                                "_type": "rel",
                                "id": getattr(v, "element_id", "") or "",
                                "type": getattr(v, "type", "") or "",
                                "properties": dict(v),
                            }
                            # start/end element ids (string)
                            try:
                                rec[k]["start"] = getattr(v.start_node, "element_id", "") or ""
                                rec[k]["end"] = getattr(v.end_node, "element_id", "") or ""
                            except Exception:
                                pass
                            continue
                    except Exception:
                        pass

                    # dict-like or primitive
                    if isinstance(v, dict):
                        rec[k] = v
                    else:
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
