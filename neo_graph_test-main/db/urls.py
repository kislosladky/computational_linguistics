from django.urls import path

from . import views

from db.views import (
    getTest,
    postTest,
    deleteTest
)

urlpatterns = [
    path('getTest',getTest , name='getTest'),
    path('postTest',postTest , name='postTest'),
    path('deleteTest',deleteTest , name='deleteTest'),

    # Corpus
    path("corpus/create", views.create_corpus, name="create_corpus"),
    path("corpus/<int:corpus_id>", views.get_corpus, name="get_corpus"),
    path("corpus/<int:corpus_id>/update", views.update_corpus, name="update_corpus"),
    path("corpus/<int:corpus_id>/delete", views.delete_corpus, name="delete_corpus"),

    # Text
    path("text/create", views.create_text, name="create_text"),
    path("text/<int:text_id>", views.get_text, name="get_text"),
    path("text/<int:text_id>/update", views.update_text, name="update_text"),
    path("text/<int:text_id>/delete", views.delete_text, name="delete_text"),

    #Ontology
    path("ontology", views.get_ontology, name="get_ontology"),
    path("ontology/parents", views.get_ontology_parents, name="get_ontology_parents"),

    # Class
    path("class/create", views.create_class, name="create_class"),
    path("class/<str:uri>", views.get_class, name="get_class"),
    path("class/<str:uri>/parents", views.get_class_parents, name="get_class_parents"),
    path("class/<str:uri>/children", views.get_class_children, name="get_class_children"),
    path("class/<str:uri>/objects", views.get_class_objects, name="get_class_objects"),
    path("class/<str:uri>/update", views.update_class, name="update_class"),
    path("class/<str:uri>/delete", views.delete_class, name="delete_class"),

    # Object
    path("object/create", views.create_object, name="create_object"),
    path("object/<str:uri>", views.get_object, name="get_object"),
    path("object/<str:uri>/update", views.update_object, name="update_object"),
    path("object/<str:uri>/delete", views.delete_object, name="delete_object"),

    # Class attributes
    path("class/<str:uri>/attribute/add", views.add_class_attribute, name="add_class_attribute"),
    path("class/<str:uri>/attribute/<str:attr_name>/delete", views.delete_class_attribute, name="delete_class_attribute"),

    # Object attributes
    path("class/<str:uri>/object-attribute/add", views.add_class_object_attribute, name="add_class_object_attribute"),
    path("class/object-attribute/<str:object_property_uri>/delete", views.delete_class_object_attribute, name="delete_class_object_attribute"),

    # Add parent
    path("class/<str:uri>/add-parent", views.add_class_parent, name="add_class_parent"),

    # Signature
    path("class/<str:uri>/collect-signature", views.collect_signature, name="collect_signature"),

    # Embeddings
    path("compare/<int:id1>/<int:id2>", views.compare_texts, name="compare_texts"),
]