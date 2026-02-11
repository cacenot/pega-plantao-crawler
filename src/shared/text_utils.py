"""Utilitários de formatação de texto."""

# Preposições e artigos que devem permanecer em minúsculo no Title Case
_LOWERCASE_WORDS = frozenset(
    {
        "de",
        "da",
        "do",
        "dos",
        "das",
        "e",
        "em",
        "na",
        "no",
        "nas",
        "nos",
        "para",
        "por",
        "com",
        "sem",
        "sob",
        "ao",
        "aos",
        "à",
        "às",
    }
)


def _capitalize_word(word: str, is_first: bool) -> str:
    """Capitaliza uma palavra, tratando '/' como separador interno.

    Exemplo:
        >>> _capitalize_word("cancerologia/cancerologia", True)
        'Cancerologia/Cancerologia'
    """
    if "/" in word:
        parts = word.split("/")
        return "/".join(
            _capitalize_word(p, is_first=(is_first and j == 0))
            for j, p in enumerate(parts)
        )

    lower = word.lower()
    if is_first or lower not in _LOWERCASE_WORDS:
        return word.capitalize()
    return lower


def title_case_br(text: str | None) -> str | None:
    """Converte texto para Title Case respeitando preposições do português.

    Trata '/' como separador de palavras, capitalizando cada segmento.

    Exemplo:
        >>> title_case_br("UNIVERSIDADE FEDERAL DO PARANA")
        'Universidade Federal do Parana'
        >>> title_case_br("JOSE DA SILVA DOS SANTOS")
        'Jose da Silva dos Santos'
        >>> title_case_br("CANCEROLOGIA/CANCEROLOGIA PEDIÁTRICA")
        'Cancerologia/Cancerologia Pediátrica'
    """
    if not text:
        return text

    words = text.strip().split()
    if not words:
        return text

    return " ".join(
        _capitalize_word(word, is_first=(i == 0)) for i, word in enumerate(words)
    )
