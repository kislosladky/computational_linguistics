from django.shortcuts import render
from django.http import StreamingHttpResponse, HttpResponseRedirect, HttpResponse
from django.forms.models import model_to_dict
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
import datetime
from django.db.models import Q
from.onthology_namespace import *
from .models import Test
from core.settings import *

# API IMPORTS
from rest_framework.response import Response
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated, AllowAny

# REPO IMPORTS
from db.api.TestRepository import TestRepository


from rest_framework import status

from .dao import CorpusDAO, TextDAO
from .serializers import CorpusSerializer, TextSerializer
from .models import Corpus, Text
from .api.ontology import OntologyService
from .api.repository import Neo4jRepository

from pprint import pprint

@api_view(['GET', ])
@permission_classes((AllowAny,))
def getTest(request):
    id = request.GET.get('id', None)
    if id is None:
        return HttpResponse(status=400)
    
    testRepo = TestRepository()
    result = testRepo.getTest(id = id)
    return Response(result)

@api_view(['POST', ])
@permission_classes((AllowAny,))
def postTest(request):
    data = json.loads(request.body.decode('utf-8'))
    testRepo = TestRepository()
    test = testRepo.postTest(test_data = data)
    return JsonResponse(test)

@api_view(['DELETE', ])
@permission_classes((AllowAny,))
def deleteTest(request):
    id = request.GET.get('id', None)
    if id is None:
        return HttpResponse(status=400)
    
    testRepo = TestRepository()
    result = testRepo.deleteTest(id = id)
    return Response(result)




# ---------- Corpus ----------

@api_view(["POST"])
@permission_classes((AllowAny,))
def create_corpus(request):
    data = request.data
    corpus = CorpusDAO.create_corpus(
        name=data.get("name"),
        description=data.get("description"),
        genre=data.get("genre"),
    )
    return Response(CorpusSerializer(corpus).data, status=status.HTTP_201_CREATED)


@api_view(["PUT"])
@permission_classes((AllowAny,))
def update_corpus(request, corpus_id):
    data = request.data
    corpus = CorpusDAO.update_corpus(corpus_id, **data)
    return Response(CorpusSerializer(corpus).data)


@api_view(["GET"])
@permission_classes((AllowAny,))
def get_corpus(request, corpus_id):
    corpus = CorpusDAO.get_corpus(corpus_id)
    return Response(CorpusSerializer(corpus).data)


@api_view(["DELETE"])
@permission_classes((AllowAny,))
def delete_corpus(request, corpus_id):
    CorpusDAO.delete_corpus(corpus_id)
    return Response({"message": "Corpus deleted"}, status=status.HTTP_204_NO_CONTENT)


# ---------- Text ----------

@api_view(["POST"])
@permission_classes((AllowAny,))
def create_text(request):
    data = request.data
    corpus = Corpus.objects.get(id=data.get("corpus"))
    has_translation = None
    if data.get("has_translation"):
        has_translation = Text.objects.get(id=data.get("has_translation"))

    text = TextDAO.create_text(
        name=data.get("name"),
        description=data.get("description"),
        content=data.get("content"),
        corpus=corpus,
        has_translation=has_translation,
    )
    return Response(TextSerializer(text).data, status=status.HTTP_201_CREATED)


@api_view(["PUT"])
@permission_classes((AllowAny,))
def update_text(request, text_id):
    data = request.data
    text = TextDAO.update_text(
        text_id,
        name=data.get("name"),
        description=data.get("description"),
        content=data.get("content"),
        corpus_id=data.get("corpus")
    )
    return Response(TextSerializer(text).data)


@api_view(["GET"])
@permission_classes((AllowAny,))
def get_text(request, text_id):
    text = TextDAO.get_text(text_id)
    return Response(TextSerializer(text).data)


@api_view(["DELETE"])
@permission_classes((AllowAny,))
def delete_text(request, text_id):
    TextDAO.delete_text(text_id)
    return Response({"message": "Text deleted"}, status=status.HTTP_204_NO_CONTENT)




# Создаем сервис (лучше потом вынести в DI контейнер / singleton)
repo = Neo4jRepository()
service = OntologyService(repo)


# ---------- Ontology ----------

@api_view(["GET"])
@permission_classes((AllowAny,))
def get_ontology(request):
    data = service.get_ontology()
    return Response(data)


@api_view(["GET"])
@permission_classes((AllowAny,))
def get_ontology_parents(request):
    data = service.get_ontology_parent_classes()
    return Response(data)


# ---------- Class ----------

@api_view(["GET"])
@permission_classes((AllowAny,))
def get_class(request, uri: str):
    data = service.get_class(uri)
    return Response(data if data else {}, status=status.HTTP_200_OK)


@api_view(["GET"])
@permission_classes((AllowAny,))
def get_class_parents(request, uri: str):
    data = service.get_class_parents(uri)
    return Response(data)


@api_view(["GET"])
@permission_classes((AllowAny,))
def get_class_children(request, uri: str):
    data = service.get_class_children(uri)
    return Response(data)


@api_view(["GET"])
@permission_classes((AllowAny,))
def get_class_objects(request, uri: str):
    data = service.get_class_objects(uri)
    return Response(data)


@api_view(["POST"])
@permission_classes((AllowAny,))
def create_class(request):
    title = request.data.get("title")
    description = request.data.get("description", "")
    uri = request.data.get("uri")
    parent_uri = request.data.get("parent_uri")
    node = service.create_class(title, description, uri, parent_uri)
    return Response(node, status=status.HTTP_201_CREATED)


@api_view(["PUT"])
@permission_classes((AllowAny,))
def update_class(request, uri: str):
    title = request.data.get("title")
    description = request.data.get("description")
    node = service.update_class(uri, title, description)
    return Response(node)


@api_view(["DELETE"])
@permission_classes((AllowAny,))
def delete_class(request, uri: str):
    stats = service.delete_class(uri)
    return Response(stats)


# ---------- Object ----------

@api_view(["GET"])
@permission_classes((AllowAny,))
def get_object(request, uri: str):
    obj = service.get_object(uri)
    return Response(obj)


@api_view(["POST"])
@permission_classes([AllowAny])
def create_object(request):
    class_uri = request.data.get("class_uri")
    if not class_uri:
        return Response({"error": "class_uri is required"}, status=status.HTTP_400_BAD_REQUEST)

    props = request.data.get("properties", {})

    relations = request.data.get("relations", {})

    # Создаём объект через OntologyService
    node = service.create_object(class_uri, props, relations)

    # Node уже словарь с _type и properties, безопасный для JSON
    return Response(node, status=status.HTTP_201_CREATED)


@api_view(["PUT"])
@permission_classes((AllowAny,))
def update_object(request, uri: str):
    props = request.data.get("properties", {})
    node = service.update_object(uri, props)
    return Response(node)


@api_view(["DELETE"])
@permission_classes((AllowAny,))
def delete_object(request, uri: str):
    deleted = service.delete_object(uri)
    return Response({"deleted": deleted})

# ---------- Class Attribute ----------
@api_view(["POST"])
@permission_classes((AllowAny,))
def add_class_attribute(request, uri):
    data = json.loads(request.body)
    attr_name = data.get("name")
    attr_type = data.get("type")
    attr_type = {"type": attr_type}  # например: string, int
    res = service.add_class_attribute(uri, attr_name, attr_props=attr_type)
    return JsonResponse(res, safe=False)

@api_view(["DELETE"])
@permission_classes((AllowAny,))
def delete_class_attribute(request, uri, attr_name):
    res = service.delete_class_attribute(uri, attr_name)
    return JsonResponse({"deleted": res})


# ---------- Object Attribute ----------
@api_view(["POST"])
@permission_classes((AllowAny,))
def add_class_object_attribute(request, uri):
    data = json.loads(request.body)
    attr_name = data.get("name")
    range_class_uri = data.get("range_class_uri")
    res = service.add_class_object_attribute(uri, attr_name, range_class_uri)
    return JsonResponse(res, safe=False)

@api_view(["DELETE"])
@permission_classes((AllowAny,))
def delete_class_object_attribute(request, object_property_uri):
    res = service.delete_class_object_attribute(object_property_uri)
    return JsonResponse({"deleted": res})


# ---------- Add Parent ----------
@api_view(["POST"])
@permission_classes((AllowAny,))
def add_class_parent(request, uri):
    data = json.loads(request.body)
    parent_uri = data.get("parent_uri")
    res = service.add_class_parent(parent_uri, uri)
    return JsonResponse(res, safe=False)


# ---------- Collect Signature ----------
@api_view(["GET"])
@permission_classes((AllowAny,))
def collect_signature(request, uri):
    sig = service.collect_signature(uri)
    return JsonResponse(sig)
