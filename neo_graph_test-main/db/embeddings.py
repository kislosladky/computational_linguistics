from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import re

# Загружаем модель один раз при импорте
model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")


def get_chunks(text, max_words=100):
    """
    Разбивает длинный текст на фрагменты примерно по max_words слов.
    Возвращает список строк.
    """
    words = re.findall(r'\S+', text)
    chunks = []
    for i in range(0, len(words), max_words):
        chunk = " ".join(words[i:i + max_words])
        chunks.append(chunk)
    return chunks


def get_embeddings(texts):
    """
    Принимает один текст или список текстов.
    Возвращает векторы эмбеддингов (numpy array).
    """
    if isinstance(texts, str):
        texts = [texts]
    embeddings = model.encode(texts, convert_to_numpy=True)
    return embeddings


def cos_compare(emb1, emb2):
    """
    Вычисляет косинусное сходство между двумя эмбеддингами.
    Возвращает значение от -1 до 1.
    """
    emb1 = np.array(emb1).reshape(1, -1)
    emb2 = np.array(emb2).reshape(1, -1)
    return float(cosine_similarity(emb1, emb2)[0][0])
