
from datetime import datetime


def get_last_month():
    now = datetime.now()
    last_month = now.replace(
        month=(now.month - 1),
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )
    return last_month