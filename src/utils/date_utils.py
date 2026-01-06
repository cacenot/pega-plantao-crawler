"""Utilitários para manipulação de datas."""

from datetime import datetime
from calendar import monthrange


def get_date_range() -> tuple[str, str]:
    """
    Retorna o range de datas: hoje até o fim do próximo mês.

    Returns:
        Tupla com (start_date, end_date) nos formatos esperados pela API.
        - start_date: "YYYY-MM-DD"
        - end_date: "YYYY-MM-DDTHH:MM:SS"
    """
    today = datetime.now()

    # Calcula o próximo mês
    if today.month == 12:
        next_month = 1
        next_year = today.year + 1
    else:
        next_month = today.month + 1
        next_year = today.year

    # Último dia do próximo mês
    last_day = monthrange(next_year, next_month)[1]
    end_of_next_month = datetime(next_year, next_month, last_day)

    start_date = today.strftime("%Y-%m-%d")
    end_date = end_of_next_month.strftime("%Y-%m-%dT00:00:00")

    return start_date, end_date
