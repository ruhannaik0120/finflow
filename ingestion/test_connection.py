"""Manual Snowflake connectivity check (not part of the unittest suite)."""

from pathlib import Path
import sys

from dotenv import load_dotenv

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.pipeline_utils import create_snowflake_connection  # noqa: E402


def main() -> int:
    load_dotenv()
    connection = create_snowflake_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT CURRENT_VERSION()")
        cursor.fetchone()
        print("Connected successfully to Snowflake.")
        return 0
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
