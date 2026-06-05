from .clinical  import get_clinical_rules
from .sppb      import get_sppb_rules
from .lifestyle import get_lifestyle_rules
from .gait      import get_gait_rules

_LOADERS = {
    "clinical":  get_clinical_rules,
    "sppb":      get_sppb_rules,
    "lifestyle": get_lifestyle_rules,
    "gait":      get_gait_rules,
}

VALID_TAGS = frozenset(_LOADERS)


def get_rules(tag: str) -> dict[str, str]:
    """
    Devuelve {nombre: constraint} para todas las reglas de la fuente indicada.

    El dict resultante es la entrada directa de apply_rules_and_split() en Silver.
    """
    if tag not in _LOADERS:
        raise ValueError(f"Tag desconocido: {tag!r}. Válidos: {sorted(VALID_TAGS)}")
    all_rules = (
        get_clinical_rules()
        + get_sppb_rules()
        + get_lifestyle_rules()
        + get_gait_rules()
    )
    return {r["name"]: r["constraint"] for r in all_rules if r["tag"] == tag}
