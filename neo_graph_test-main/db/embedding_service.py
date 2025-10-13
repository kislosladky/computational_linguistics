from .models import Text
from .embeddings import get_embeddings, cos_compare
import numpy as np

def compare_texts_by_ids(id1, id2):
    t1 = Text.objects.get(pk=id1)
    t2 = Text.objects.get(pk=id2)

    emb1 = get_embeddings(t1.content)[0]
    emb2 = get_embeddings(t2.content)[0]

    score = cos_compare(emb1, emb2)
    similarity = np.clip(score, -1.0, 1.0)

    return similarity