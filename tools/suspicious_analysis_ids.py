from __future__ import annotations

import sqlite3

from syuka_ops.audit import collect_analysis_quality_rows


def main() -> None:
    conn = sqlite3.connect("/data/db/syuka_ops.db")
    conn.row_factory = sqlite3.Row
    try:
        rows = collect_analysis_quality_rows(conn, limit=1000)
        for row in rows:
            print(row["video_id"])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
