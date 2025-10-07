from django.db import models
from db_file_storage.model_utils import delete_file, delete_file_if_needed


class Test(models.Model):
    name = models.TextField()

    def __str__(self):
        return self.name  # Returns the value of the 'name' field
    

from django.db import models


class Corpus(models.Model):
    """Корпус текстов"""
    name = models.CharField(max_length=255, verbose_name="Название")
    description = models.TextField(blank=True, null=True, verbose_name="Описание")
    genre = models.CharField(max_length=100, verbose_name="Жанр")

    def __str__(self):
        return self.name


class Text(models.Model):
    """Текст, принадлежащий корпусу"""
    name = models.CharField(max_length=255, verbose_name="Название")
    description = models.TextField(blank=True, null=True, verbose_name="Описание")
    content = models.TextField(verbose_name="Текст")

    corpus = models.ForeignKey(
        Corpus,
        on_delete=models.CASCADE,
        related_name="texts",
        verbose_name="Корпус"
    )

    has_translation = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="translations",
        verbose_name="Перевод"
    )

    def __str__(self):
        return self.name

    