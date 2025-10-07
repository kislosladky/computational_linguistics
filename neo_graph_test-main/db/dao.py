from .models import Corpus, Text
from django.shortcuts import get_object_or_404


class CorpusDAO:
    """DAO для работы с таблицей Corpus"""

    @staticmethod
    def create_corpus(name, description=None, genre=None):
        return Corpus.objects.create(name=name, description=description, genre=genre)

    @staticmethod
    def update_corpus(corpus_id, **kwargs):
        corpus = get_object_or_404(Corpus, id=corpus_id)
        for field, value in kwargs.items():
            setattr(corpus, field, value)
        corpus.save()
        return corpus

    @staticmethod
    def get_corpus(corpus_id):
        return get_object_or_404(Corpus, id=corpus_id)

    @staticmethod
    def delete_corpus(corpus_id):
        corpus = get_object_or_404(Corpus, id=corpus_id)
        corpus.delete()
        return True


class TextDAO:
    """DAO для работы с таблицей Text"""

    @staticmethod
    def create_text(name, content, corpus, description=None, has_translation=None):
        return Text.objects.create(
            name=name,
            description=description,
            content=content,
            corpus=corpus,
            has_translation=has_translation
        )

    @staticmethod
    def update_text(text_id, **kwargs):
        text = get_object_or_404(Text, id=text_id)

        # если передан corpus_id — ищем объект корпуса
        corpus_id = kwargs.pop("corpus_id", None)
        if corpus_id is not None:
            corpus = get_object_or_404(Corpus, id=corpus_id)
            text.corpus = corpus

        # обновляем остальные поля
        for field, value in kwargs.items():
            setattr(text, field, value)

        text.save()
        return text


    @staticmethod
    def get_text(text_id):
        return get_object_or_404(Text, id=text_id)

    @staticmethod
    def delete_text(text_id):
        text = get_object_or_404(Text, id=text_id)
        text.delete()
        return True
