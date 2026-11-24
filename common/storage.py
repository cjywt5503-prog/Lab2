import sqlite3
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def database(path: str):
    """Окреме з'єднання на операцію; файл спільний для реплік одного сервісу."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=10)
    connection.row_factory = sqlite3.Row
    try:
        with connection:
            yield connection
    finally:
        connection.close()
