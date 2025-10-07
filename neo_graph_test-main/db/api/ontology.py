from typing import Any, Dict, List, Optional

from .repository import Neo4jRepository
from pprint import pprint

SUBCLASS_REL = "SUBCLASS_OF"
DOMAIN_REL = "DOMAIN"
RANGE_REL = "RANGE"
TYPE_REL = "TYPE_OF"


class OntologyService:
    def __init__(self, repo: Neo4jRepository):
        self.repo = repo

    # ---------- Ontology-wide ----------
    def get_ontology(self):
        """Возвращает все узлы и связи"""
        return self.repo.get_all_nodes_and_arcs()

    def get_ontology_parent_classes(self):
        q = f"""
        MATCH (c:Class)
        WHERE NOT ( (c)-[:{SUBCLASS_REL}]->() )
        RETURN c
        """
        return self.repo.run_custom_query(q)

    # ---------- Class queries ----------
    def get_class(self, class_uri: str):
        q = "MATCH (c:Class {uri:$uri}) RETURN c LIMIT 1"
        rows = self.repo.run_custom_query(q, {"uri": class_uri})
        return rows[0]["c"] if rows else None

    def get_class_parents(self, class_uri: str):
        q = f"""
        MATCH (c:Class {{uri:$uri}})-[:{SUBCLASS_REL}*]->(p:Class)
        RETURN p
        """
        return [r["p"] for r in self.repo.run_custom_query(q, {"uri": class_uri})]

    def get_class_children(self, class_uri: str):
        q = f"""
        MATCH (child:Class)-[:{SUBCLASS_REL}*]->(c:Class {{uri:$uri}})
        RETURN child
        """
        return [r["child"] for r in self.repo.run_custom_query(q, {"uri": class_uri})]

    def get_class_objects(self, class_uri: str):
        q = f"""
        MATCH (o:Object)-[:{TYPE_REL}]->(c:Class {{uri:$uri}})
        RETURN o
        """
        rows = self.repo.run_custom_query(q, {"uri": class_uri})
        if not rows:
            q2 = "MATCH (o:Object {class_uri:$uri}) RETURN o"
            rows = self.repo.run_custom_query(q2, {"uri": class_uri})
        return [r["o"] for r in rows]

    # ---------- Class lifecycle ----------
    def create_class(self, title: str, description: str = "", uri: str = None, parent_uri: str = None):
        props = {"title": title, "description": description}
        if uri:
            props["uri"] = uri
        node = self.repo.create_node(props, labels=["Class"])
        if parent_uri:
            self.repo.create_arc(node["uri"], parent_uri, rel_type=SUBCLASS_REL)
        return node

    def update_class(self, class_uri: str, title: str = None, description: str = None):
        props = {}
        if title is not None:
            props["title"] = title
        if description is not None:
            props["description"] = description
        return self.repo.update_node(class_uri, props, merge=True) if props else self.get_class(class_uri)

    def delete_class(self, class_uri: str) -> Dict[str, int]:
        """
        Удаляет класс и рекурсивно: всех потомков-классов и все объекты этих классов.
        Кроме того удаляет DatatypeProperty и ObjectProperty, связанные с этими классами.
        Для ObjectProperty предварительно удаляет рёбра между объектами типа op_uri.
        Возвращает статистику: {"classes_deleted": n, "objects_deleted": m,
                                 "dp_deleted": x, "op_deleted": y, "relations_deleted": z}
        """
        stats = {
            "classes_deleted": 0,
            "objects_deleted": 0,
            "dp_deleted": 0,
            "op_deleted": 0,
            "relations_deleted": 0
        }

        # 1) Получаем root + всех дочерних классов (desc) как отдельные строки
        q_desc = f"""
        MATCH (root:Class {{uri:$uri}})
        OPTIONAL MATCH (desc:Class)-[:{SUBCLASS_REL}*]->(root)
        RETURN root, desc
        """
        rows = self.repo.run_custom_query(q_desc, {"uri": class_uri})
        if not rows:
            return stats

        # 2) Собираем уникальные uri всех классов (root + desc)
        class_uris = set()
        for r in rows:
            root_node = r.get("root")
            if root_node and isinstance(root_node, dict):
                ru = root_node.get("properties", {}).get("uri")
                if ru:
                    class_uris.add(ru)
            desc_node = r.get("desc")
            if desc_node and isinstance(desc_node, dict):
                du = desc_node.get("properties", {}).get("uri")
                if du:
                    class_uris.add(du)

        if not class_uris:
            return stats

        # 3) Найдём ObjectProperty и DatatypeProperty, связанные с этими классами
        # ObjectProperty (по DOMAIN или обратной связи)
        q_ops = f"""
        UNWIND $uris AS cu
        OPTIONAL MATCH (op:ObjectProperty)-[:{DOMAIN_REL}]->(c:Class {{uri:cu}})
        WITH collect(DISTINCT op) AS ops1

        UNWIND $uris AS cu
        OPTIONAL MATCH (c2:Class {{uri:cu}})-[:{DOMAIN_REL}]->(op2:ObjectProperty)
        WITH ops1, collect(DISTINCT op2) AS ops2

        WITH [o IN ops1 WHERE o IS NOT NULL] + [o IN ops2 WHERE o IS NOT NULL] AS all_ops
        UNWIND all_ops AS opnode
        RETURN DISTINCT opnode
        """
        rop = self.repo.run_custom_query(q_ops, {"uris": list(class_uris)})
        op_uris = []
        if rop:
            for row in rop:
                opnode = row.get("opnode")
                if opnode and isinstance(opnode, dict):
                    u = opnode.get("properties", {}).get("uri")
                    if u:
                        op_uris.append(u)

        # DatatypeProperty (по DOMAIN)
        q_dps = f"""
        UNWIND $uris AS cu
        OPTIONAL MATCH (dp:DatatypeProperty)-[:{DOMAIN_REL}]->(c:Class {{uri:cu}})
        WITH collect(DISTINCT dp) AS dps1

        UNWIND $uris AS cu  
        OPTIONAL MATCH (c2:Class {{uri:cu}})-[:{DOMAIN_REL}]->(dp2:DatatypeProperty)
        WITH dps1, collect(DISTINCT dp2) AS dps2

        WITH [d IN dps1 WHERE d IS NOT NULL] + [d IN dps2 WHERE d IS NOT NULL] AS all_dps
        UNWIND all_dps AS dpnode
        RETURN DISTINCT dpnode
        """
        rdp = self.repo.run_custom_query(q_dps, {"uris": list(class_uris)})
        dp_uris = []
        if rdp:
            for row in rdp:
                dpnode = row.get("dpnode")
                if dpnode and isinstance(dpnode, dict):
                    u = dpnode.get("properties", {}).get("uri")
                    if u:
                        dp_uris.append(u)

        # 4) Удаляем рёбра между объектами для каждого op_uri (type(r) = op_uri)
        for opu in op_uris:
            if not opu:
                continue
            # Исправлено: используем динамический запрос с конкатенацией
            q_del_rel = f"MATCH ()-[r:`{opu}`]->() DELETE r RETURN count(r) AS cnt"
            rows_rel = self.repo.run_custom_query(q_del_rel)
            if rows_rel and isinstance(rows_rel[0].get("cnt"), int):
                stats["relations_deleted"] += int(rows_rel[0]["cnt"])

        # 5) Удаляем найденные ObjectProperty и DatatypeProperty узлы (detach delete)
        for opu in op_uris:
            if opu and self.repo.delete_node_by_uri(opu, detach=True):
                stats["op_deleted"] += 1

        for dpu in dp_uris:
            if dpu and self.repo.delete_node_by_uri(dpu, detach=True):
                stats["dp_deleted"] += 1

        # 6) Удаляем объекты, принадлежащие этим классам
        for cu in list(class_uris):
            q_objs = f"""
            MATCH (o:Object)-[:{TYPE_REL}]->(c:Class {{uri:$uri}})
            RETURN o
            """
            objs = self.repo.run_custom_query(q_objs, {"uri": cu})
            for row in objs:
                o_node = row.get("o")
                if o_node and isinstance(o_node, dict):
                    obj_uri = o_node.get("properties", {}).get("uri")
                    if obj_uri:
                        deleted = self.repo.delete_node_by_uri(obj_uri, detach=True)
                        if deleted:
                            stats["objects_deleted"] += int(deleted)

        # 7) Удаляем сами классы (detach delete)
        for cu in list(class_uris):
            deleted = self.repo.delete_node_by_uri(cu, detach=True)
            if deleted:
                stats["classes_deleted"] += int(deleted)

        return stats

    # ---------- DatatypeProperty ----------
    def add_class_attribute(self, class_uri: str, attr_title: str, attr_uri: str = None, attr_props: dict = None):
        props = dict(attr_props or {})
        props.setdefault("title", attr_title)
        if attr_uri:
            props["uri"] = attr_uri
        dp = self.repo.create_node(props, labels=["DatatypeProperty"])
        self.repo.create_arc(dp["uri"], class_uri, rel_type=DOMAIN_REL)
        return dp

    def delete_class_attribute(self, class_uri: str, attr_name: str = None, attr_uri: str = None):
        stats = {"attribute_node_deleted": False, "objects_touched": 0}

        # Получаем информацию об атрибуте для обоих случаев
        attr_info = None

        if attr_uri:
            q = "MATCH (dp:DatatypeProperty {uri:$attr_uri}) RETURN dp LIMIT 1"
            res = self.repo.run_custom_query(q, {"attr_uri": attr_uri})
            if res:
                attr_info = {
                    "node": res[0]["dp"],
                    "name": res[0]["dp"]["properties"].get("title")
                }
        elif attr_name:
            q = f"""
            MATCH (dp:DatatypeProperty)-[:{DOMAIN_REL}]->(c:Class {{uri:$class_uri}})
            WHERE dp.title = $attr_name
            RETURN dp LIMIT 1
            """
            res = self.repo.run_custom_query(q, {"class_uri": class_uri, "attr_name": attr_name})
            if res:
                attr_info = {
                    "node": res[0]["dp"],
                    "name": attr_name
                }
        else:
            return stats

        # Удаляем узел атрибута
        if attr_info:
            node_uri = attr_info["node"]["properties"].get("uri")
            if node_uri and self.repo.delete_node_by_uri(node_uri, detach=True):
                stats["attribute_node_deleted"] = True

            # Очищаем поле у объектов (работает для обоих случаев)
            if attr_info["name"]:
                q_clear = f"""
                MATCH (root:Class {{uri:$class_uri}})
                OPTIONAL MATCH (desc:Class)-[:{SUBCLASS_REL}*]->(root)
                WITH collect(root) + collect(desc) AS classes
                UNWIND classes AS cl
                MATCH (o:Object)-[:{TYPE_REL}]->(cl)
                SET o[$attr_name] = null
                RETURN count(DISTINCT o) AS cnt
                """
                rows = self.repo.run_custom_query(q_clear, {
                    "class_uri": class_uri,
                    "attr_name": attr_info["name"]
                })
                stats["objects_touched"] = rows[0]["cnt"] if rows else 0

        return stats

    # ---------- ObjectProperty ----------
    def add_class_object_attribute(self,
                                   class_uri: str,
                                   attr_name: str,
                                   range_class_uri: str,
                                   attr_uri: str = None,
                                   attr_props: dict = None):
        props = dict(attr_props or {})
        props.setdefault("title", attr_name)
        if attr_uri:
            props["uri"] = attr_uri
        else:
            props["uri"] = self.repo.generate_random_string(12)
        op = self.repo.create_node(props, labels=["ObjectProperty"])
        self.repo.create_arc(op["uri"], class_uri, rel_type=DOMAIN_REL)
        self.repo.create_arc(op["uri"], range_class_uri, rel_type=RANGE_REL)
        return op

    def delete_class_object_attribute(self, object_property_uri: str):
        stats = {"relations_deleted": 0, "property_node_deleted": False}
        q_del_rel = "MATCH ()-[r]->() WHERE type(r) = $reltype DELETE r RETURN count(r) AS cnt"
        rows = self.repo.run_custom_query(q_del_rel, {"reltype": object_property_uri})
        stats["relations_deleted"] = rows[0]["cnt"] if rows else 0
        if self.repo.delete_node_by_uri(object_property_uri, detach=True):
            stats["property_node_deleted"] = True
        return stats

    # ---------- Parent ----------
    def add_class_parent(self, parent_uri: str, target_uri: str):
        return bool(self.repo.create_arc(target_uri, parent_uri, rel_type=SUBCLASS_REL))

    # ---------- Objects ----------
    def get_object(self, object_uri: str):
        q = "MATCH (o:Object {uri:$uri}) RETURN o LIMIT 1"
        rows = self.repo.run_custom_query(q, {"uri": object_uri})
        return rows[0]["o"] if rows else None

    def delete_object(self, object_uri: str):
        return self.repo.delete_node_by_uri(object_uri, detach=True) > 0

    def create_object(self, class_uri: str, properties: dict, relations: Optional[List[Dict[str, Any]]] = None):
        # Получаем сигнатуру класса для валидации
        signature = self.collect_signature(class_uri)

        # Валидируем свойства
        validated_props = self._validate_properties(properties, signature)

        props = dict(validated_props)
        if not props.get("uri"):
            props["uri"] = self.repo.generate_random_string(12)
        node = self.repo.create_node(props, labels=["Object"])
        self.repo.create_arc(node["uri"], class_uri, rel_type=TYPE_REL)

        # Создаём связи из аргумента relations
        relations = relations or []
        for rel in relations:
            direction = rel.get("direction") or 1
            target_uri = rel.get("target_uri")
            rel_uri = rel["rel_uri"]

            if not target_uri or not rel_uri:
                continue

            # достаём узел класса связи
            rel_node = self.repo.get_node_by_uri(rel_uri)
            if not rel_node:
                continue  # если связи нет в онтологии → пропускаем

            rel_type = dict(rel_node)
            print(rel_type )
            rel_type = rel_type.get("properties").get("title")
            if not rel_type:
                continue  # если в узле не задано поле type → тоже пропускаем

            # создаём связь с учётом направления
            if direction == 1:
                self.repo.create_arc(
                    node1_uri=node["uri"],
                    node2_uri=target_uri,
                    rel_type=rel_type
                )
            elif direction == -1:
                self.repo.create_arc(
                    node1_uri=target_uri,
                    node2_uri=node["uri"],
                    rel_type=rel_type
                )

        return node

    def update_object(self, object_uri: str, properties: dict):
        # Получаем класс объекта
        q = f"""
        MATCH (o:Object {{uri:$uri}})-[:{TYPE_REL}]->(c:Class)
        RETURN c.uri AS class_uri
        """
        result = self.repo.run_custom_query(q, {"uri": object_uri})

        if not result:
            raise ValueError(f"Object {object_uri} not found or has no class")

        class_uri = result[0]["class_uri"]
        signature = self.collect_signature(class_uri)

        # Валидируем свойства
        validated_props = self._validate_properties(properties, signature)

        return self.repo.update_node(object_uri, validated_props, merge=True)

    def _validate_properties(self, properties: dict, signature: dict) -> dict:
        """
        Валидирует свойства объекта на основе сигнатуры класса.
        Возвращает только разрешенные свойства.
        """
        validated_props = {}

        # Разрешенные datatype properties
        allowed_dp = {dp.get("title") for dp in signature.get("datatype_properties", []) if dp.get("title")}

        # Разрешенные object properties (только имена свойств)
        allowed_op = {op.get("title") for op in signature.get("object_properties", []) if op.get("title")}

        # Служебные свойства, которые всегда разрешены
        system_props = {"uri", "title", "description"}

        all_allowed_props = allowed_dp | allowed_op | system_props

        # Фильтруем свойства
        for prop_name, prop_value in properties.items():
            if prop_name in all_allowed_props:
                validated_props[prop_name] = prop_value
            else:
                print(f"Warning: Property '{prop_name}' is not allowed for this class")

        return validated_props


    # ---------- Signature ----------
    def collect_signature(self, class_uri: str) -> dict:
        """
        Возвращает сигнатуру класса в виде словаря, готового к JSON:
        {
            "datatype_properties": [{"id": ..., "title": ..., "description": ...}, ...],
            "object_properties": [{"id": ..., "title": ..., "description": ..., "range": {...}}, ...]
        }
        """
        signature = {"datatype_properties": [], "object_properties": []}

        # ---------- DatatypeProperties ----------

        q_dp = """
        MATCH (c:Class {uri:$uri})
        OPTIONAL MATCH (dp:DatatypeProperty)-[:DOMAIN]->(c)
        RETURN collect(dp) AS dps
        """
        rdp = self.repo.run_custom_query(q_dp, {"uri": class_uri})
        print(rdp)
        dps = rdp[0]["dps"] if rdp else []
        for dp in dps:
            if not dp:
                continue
            props = dict(dp)
            signature["datatype_properties"].append({
                "id": props.get("uri"),
                "title": props.get("title"),
                "description": props.get("description"),
                **props
            })

        # ---------- ObjectProperties ----------
        q_op = f"""
        MATCH (c:Class {{uri:$uri}})
        OPTIONAL MATCH (op:ObjectProperty)-[:{DOMAIN_REL}]->(c)
        OPTIONAL MATCH (op)-[:{RANGE_REL}]->(range:Class)
        RETURN collect(op) AS ops, collect(range) AS ranges
        """
        rop = self.repo.run_custom_query(q_op, {"uri": class_uri})
        if rop:
            ops = rop[0]["ops"]
            ranges = rop[0]["ranges"]
            for i, op in enumerate(ops):
                if not op:
                    continue
                op_props = dict(op)
                range_node = ranges[i] if i < len(ranges) else None
                range_props = dict(range_node) if range_node else None
                signature["object_properties"].append({
                    "id": op_props.get("uri"),
                    "title": op_props.get("title"),
                    "description": op_props.get("description"),
                    **op_props,
                    "range": {
                        "id": range_props.get("uri"),
                        "title": range_props.get("title"),
                        "description": range_props.get("description"),
                        **(range_props or {})
                    } if range_props else None
                })

        return signature


if __name__ == "__main__":
    repo = Neo4jRepository()
    service = OntologyService(repo)
    try:
        # 1. Создать классы
        animal = service.create_class("Animal", "Животное")
        dog = service.create_class("Dog", "Собака", parent_uri=animal["properties"]["uri"])
        print("\n=== Created classes ===")
        pprint(animal)
        pprint(dog)

        # 2. Добавить атрибут (DatatypeProperty) к Animal
        name_attr = service.add_class_attribute(animal["properties"]["uri"], "name")
        print("\n=== Added DatatypeProperty ===")
        pprint(name_attr)

        # service.delete_class_attribute(animal["properties"]["uri"], "name")
        # print("\n=== Deleted DatatypeProperty ===")

        # 3. Добавить ObjectProperty Dog -> Animal (например "owner")

        person = service.create_class("Person", "Человек")
        owner_attr = service.add_class_object_attribute(
            dog["properties"]["uri"], "owner", person["properties"]["uri"]
        )
        print("\n=== Added ObjectProperty ===")
        pprint(owner_attr)

        # 4. Создать объекты
        rex = service.create_object(dog["properties"]["uri"], {"name": "Rex"})
        bobik = service.create_object(dog["properties"]["uri"], {"name": "Bobik"})
        print("\n=== Created objects ===")
        pprint(rex)
        pprint(bobik)

        # 5. Получить объекты класса Dog
        dog_objects = service.get_class_objects(dog["properties"]["uri"])
        print("\n=== Dog objects ===")
        pprint(dog_objects)

        # 6. Получить сигнатуру класса Dog
        dog_signature = service.collect_signature(dog["properties"]["uri"])
        print("\n=== Dog signature ===")
        pprint(dog_signature)

        # 7. Получить все корневые классы
        roots = service.get_ontology_parent_classes()
        print("\n=== Ontology parent classes ===")
        pprint(roots)

        # 8. Получить всю онтологию
        ontology = service.get_ontology()
        print("\n=== Full ontology (sample) ===")
        pprint(ontology[:2])  # для краткости — первые 2 узла

        # 9. Удалить класс Animal (удалит Dog и объекты Dog тоже)
        # stats = service.delete_class(animal["properties"]["uri"])
        # print("\n=== Delete stats ===")
        # pprint(stats)

    finally:
        repo.close()