from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable


def normalized_search_query(query: str) -> str:
    return "".join(str(query or "").split())


def connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=60.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 60000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS videos (
            video_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            upload_date TEXT,
            view_count INTEGER,
            like_count INTEGER,
            has_ko_sub INTEGER NOT NULL DEFAULT 0,
            has_auto_ko_sub INTEGER NOT NULL DEFAULT 0,
            thumbnail_url TEXT,
            source_url TEXT,
            info_json_path TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS transcripts (
            video_id TEXT PRIMARY KEY,
            dialogue TEXT NOT NULL,
            subtitle_path TEXT,
            subtitle_source TEXT NOT NULL DEFAULT 'manual',
            segment_count INTEGER NOT NULL DEFAULT 0,
            collected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(video_id) REFERENCES videos(video_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS video_analysis (
            video_id TEXT PRIMARY KEY,
            summary TEXT,
            keywords_json TEXT,
            analysis_source TEXT NOT NULL DEFAULT 'legacy_script',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(video_id) REFERENCES videos(video_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS download_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 1,
            returncode INTEGER,
            stderr TEXT,
            stdout TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        """
    )
    existing_video_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(videos)").fetchall()
    }
    if "has_auto_ko_sub" not in existing_video_columns:
        conn.execute("ALTER TABLE videos ADD COLUMN has_auto_ko_sub INTEGER NOT NULL DEFAULT 0")

    existing_transcript_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(transcripts)").fetchall()
    }
    if "subtitle_source" not in existing_transcript_columns:
        conn.execute("ALTER TABLE transcripts ADD COLUMN subtitle_source TEXT NOT NULL DEFAULT 'manual'")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_upload_date ON videos(upload_date DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_has_ko_sub ON videos(has_ko_sub, upload_date DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_has_auto_ko_sub ON videos(has_auto_ko_sub, upload_date DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_video_analysis_source ON video_analysis(analysis_source, updated_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_attempts_video_stage ON download_attempts(video_id, stage, created_at DESC)")

    conn.commit()


def upsert_video(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO videos (
            video_id, title, upload_date, view_count, like_count, has_ko_sub,
            has_auto_ko_sub, thumbnail_url, source_url, info_json_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            title=excluded.title,
            upload_date=excluded.upload_date,
            view_count=excluded.view_count,
            like_count=excluded.like_count,
            has_ko_sub=excluded.has_ko_sub,
            has_auto_ko_sub=excluded.has_auto_ko_sub,
            thumbnail_url=excluded.thumbnail_url,
            source_url=excluded.source_url,
            info_json_path=excluded.info_json_path,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            row["video_id"],
            row["title"],
            row.get("upload_date"),
            row.get("view_count"),
            row.get("like_count"),
            int(bool(row.get("has_ko_sub"))),
            int(bool(row.get("has_auto_ko_sub"))),
            row.get("thumbnail_url"),
            row.get("source_url"),
            row.get("info_json_path"),
        ),
    )


def upsert_transcript(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO transcripts (video_id, dialogue, subtitle_path, subtitle_source, segment_count)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            dialogue=excluded.dialogue,
            subtitle_path=excluded.subtitle_path,
            subtitle_source=excluded.subtitle_source,
            segment_count=excluded.segment_count,
            collected_at=CURRENT_TIMESTAMP
        """,
        (
            row["video_id"],
            row["dialogue"],
            row.get("subtitle_path"),
            row.get("subtitle_source", "manual"),
            row.get("segment_count", 0),
        ),
    )


def upsert_video_analysis(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO video_analysis (video_id, summary, keywords_json, analysis_source)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            summary=excluded.summary,
            keywords_json=excluded.keywords_json,
            analysis_source=excluded.analysis_source,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            row["video_id"],
            row.get("summary"),
            row.get("keywords_json"),
            row.get("analysis_source", "legacy_script"),
        ),
    )


def record_attempt(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO download_attempts (
            video_id, stage, status, attempts, returncode, stderr, stdout
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["video_id"],
            row["stage"],
            row["status"],
            row.get("attempts", 1),
            row.get("returncode"),
            row.get("stderr"),
            row.get("stdout"),
        ),
    )


def latest_video_date(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT MAX(upload_date) AS latest_date FROM videos").fetchone()
    return row["latest_date"] if row and row["latest_date"] else None


def transcript_video_ids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT video_id FROM transcripts").fetchall()
    return {row["video_id"] for row in rows}


def analysis_video_ids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT video_id FROM video_analysis").fetchall()
    return {row["video_id"] for row in rows}


def failed_subtitle_video_ids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        """
        SELECT video_id, status
        FROM download_attempts
        WHERE stage = 'subtitle'
        ORDER BY created_at DESC, id DESC
        """
    ).fetchall()

    latest: dict[str, str] = {}
    for row in rows:
        if row["video_id"] not in latest:
            latest[row["video_id"]] = row["status"]
    return {video_id for video_id, status in latest.items() if status == "failed"}


def recent_videos(
    conn: sqlite3.Connection, limit: int = 10, offset: int = 0
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            v.*,
            t.segment_count,
            t.subtitle_source,
            a.summary,
            a.keywords_json,
            a.analysis_source,
            CASE WHEN t.video_id IS NULL THEN 0 ELSE 1 END AS has_transcript
        FROM videos v
        LEFT JOIN transcripts t ON t.video_id = v.video_id
        LEFT JOIN video_analysis a ON a.video_id = v.video_id
        ORDER BY v.upload_date DESC
        LIMIT ?
        OFFSET ?
        """,
        (limit, offset),
    ).fetchall()


def recent_video_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS c FROM videos").fetchone()
    return int(row["c"] or 0)


def browse_videos(
    conn: sqlite3.Connection,
    *,
    year: str | None = None,
    sort: str = "latest",
    limit: int = 10,
    offset: int = 0,
) -> list[sqlite3.Row]:
    clauses: list[str] = []
    params: list[object] = []
    if year:
        clauses.append("COALESCE(v.upload_date, '') LIKE ?")
        params.append(f"{year}-%")

    order_by = {
        "likes": "COALESCE(v.like_count, 0) DESC, v.upload_date DESC",
        "views": "COALESCE(v.view_count, 0) DESC, v.upload_date DESC",
        "latest": "v.upload_date DESC, v.video_id DESC",
    }.get(sort, "v.upload_date DESC, v.video_id DESC")

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT
            v.*,
            t.segment_count,
            t.subtitle_source,
            a.summary,
            a.keywords_json,
            a.analysis_source,
            CASE WHEN t.video_id IS NULL THEN 0 ELSE 1 END AS has_transcript
        FROM videos v
        LEFT JOIN transcripts t ON t.video_id = v.video_id
        LEFT JOIN video_analysis a ON a.video_id = v.video_id
        {where_sql}
        ORDER BY {order_by}
        LIMIT ?
        OFFSET ?
    """
    params.extend([limit, offset])
    return conn.execute(sql, params).fetchall()


def browse_video_count(conn: sqlite3.Connection, *, year: str | None = None) -> int:
    clauses: list[str] = []
    params: list[object] = []
    if year:
        clauses.append("COALESCE(upload_date, '') LIKE ?")
        params.append(f"{year}-%")
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    row = conn.execute(f"SELECT COUNT(*) AS c FROM videos {where_sql}", params).fetchone()
    return int(row["c"] or 0)


def search_videos(
    conn: sqlite3.Connection, query: str, limit: int = 10, offset: int = 0
) -> list[sqlite3.Row]:
    like = f"%{query}%"
    compact_like = f"%{normalized_search_query(query)}%"
    return conn.execute(
        """
        SELECT DISTINCT
            v.*,
            t.subtitle_source,
            a.summary,
            a.keywords_json,
            a.analysis_source,
            CASE WHEN REPLACE(v.title, ' ', '') LIKE ? THEN 1 ELSE 0 END AS title_match,
            CASE WHEN v.video_id LIKE ? THEN 1 ELSE 0 END AS video_id_match,
            CASE WHEN REPLACE(COALESCE(a.keywords_json, ''), ' ', '') LIKE ? THEN 1 ELSE 0 END AS keyword_match,
            CASE WHEN REPLACE(COALESCE(a.summary, ''), ' ', '') LIKE ? THEN 1 ELSE 0 END AS summary_match,
            CASE WHEN EXISTS (
                SELECT 1
                FROM transcripts tx
                WHERE tx.video_id = v.video_id
                  AND REPLACE(tx.dialogue, ' ', '') LIKE ?
            ) THEN 1 ELSE 0 END AS transcript_match,
            (
                CASE WHEN REPLACE(v.title, ' ', '') LIKE ? THEN 12 ELSE 0 END +
                CASE WHEN REPLACE(COALESCE(a.keywords_json, ''), ' ', '') LIKE ? THEN 8 ELSE 0 END +
                CASE WHEN REPLACE(COALESCE(a.summary, ''), ' ', '') LIKE ? THEN 5 ELSE 0 END +
                CASE WHEN EXISTS (
                    SELECT 1
                    FROM transcripts tx
                    WHERE tx.video_id = v.video_id
                      AND REPLACE(tx.dialogue, ' ', '') LIKE ?
                ) THEN 3 ELSE 0 END +
                CASE WHEN v.video_id LIKE ? THEN 2 ELSE 0 END
            ) AS relevance_score,
            CASE WHEN t.video_id IS NULL THEN 0 ELSE 1 END AS has_transcript
        FROM videos v
        LEFT JOIN transcripts t ON t.video_id = v.video_id
        LEFT JOIN video_analysis a ON a.video_id = v.video_id
        WHERE REPLACE(v.title, ' ', '') LIKE ?
           OR v.video_id LIKE ?
           OR REPLACE(COALESCE(a.summary, ''), ' ', '') LIKE ?
           OR REPLACE(COALESCE(a.keywords_json, ''), ' ', '') LIKE ?
           OR EXISTS (
                SELECT 1
                FROM transcripts tx
                WHERE tx.video_id = v.video_id
                  AND REPLACE(tx.dialogue, ' ', '') LIKE ?
           )
        ORDER BY relevance_score DESC, v.upload_date DESC
        LIMIT ?
        OFFSET ?
        """,
        (
            compact_like,
            like,
            compact_like,
            compact_like,
            compact_like,
            compact_like,
            compact_like,
            compact_like,
            compact_like,
            like,
            compact_like,
            like,
            compact_like,
            compact_like,
            compact_like,
            limit,
            offset,
        ),
    ).fetchall()


def search_videos_count(conn: sqlite3.Connection, query: str) -> int:
    like = f"%{query}%"
    compact_like = f"%{normalized_search_query(query)}%"
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM videos v
        LEFT JOIN video_analysis a ON a.video_id = v.video_id
        WHERE REPLACE(v.title, ' ', '') LIKE ?
           OR v.video_id LIKE ?
           OR REPLACE(COALESCE(a.summary, ''), ' ', '') LIKE ?
           OR REPLACE(COALESCE(a.keywords_json, ''), ' ', '') LIKE ?
           OR EXISTS (
                SELECT 1
                FROM transcripts tx
                WHERE tx.video_id = v.video_id
                  AND REPLACE(tx.dialogue, ' ', '') LIKE ?
           )
        """,
        (compact_like, like, compact_like, compact_like, compact_like),
    ).fetchone()
    return int(row["c"] or 0)


def get_video(conn: sqlite3.Connection, video_id: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT
            v.*,
            t.dialogue,
            t.segment_count,
            t.subtitle_path,
            t.subtitle_source,
            a.summary,
            a.keywords_json,
            a.analysis_source,
            CASE WHEN t.video_id IS NULL THEN 0 ELSE 1 END AS has_transcript
        FROM videos v
        LEFT JOIN transcripts t ON t.video_id = v.video_id
        LEFT JOIN video_analysis a ON a.video_id = v.video_id
        WHERE v.video_id = ?
        """,
        (video_id,),
    ).fetchone()


def transcript_snippets(
    conn: sqlite3.Connection, query: str, limit: int = 5, offset: int = 0
) -> list[sqlite3.Row]:
    like = f"%{normalized_search_query(query)}%"
    return conn.execute(
        """
        SELECT
            v.video_id,
            v.title,
            v.upload_date,
            v.view_count,
            v.like_count,
            v.thumbnail_url,
            v.source_url,
            t.subtitle_path,
            t.subtitle_source,
            a.summary,
            a.keywords_json,
            a.analysis_source,
            t.dialogue
        FROM transcripts t
        JOIN videos v ON v.video_id = t.video_id
        LEFT JOIN video_analysis a ON a.video_id = v.video_id
        WHERE REPLACE(t.dialogue, ' ', '') LIKE ?
        ORDER BY v.upload_date DESC
        LIMIT ?
        OFFSET ?
        """,
        (like, limit, offset),
    ).fetchall()


def transcript_snippets_count(conn: sqlite3.Connection, query: str) -> int:
    like = f"%{normalized_search_query(query)}%"
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM transcripts t
        WHERE REPLACE(t.dialogue, ' ', '') LIKE ?
        """,
        (like,),
    ).fetchone()
    return int(row["c"] or 0)


def video_rows_with_info_json(
    conn: sqlite3.Connection, *, limit: int = 0, offset: int = 0
) -> list[sqlite3.Row]:
    sql = """
        SELECT
            video_id,
            title,
            upload_date,
            view_count,
            like_count,
            thumbnail_url,
            source_url,
            info_json_path
        FROM videos
        WHERE info_json_path IS NOT NULL
          AND TRIM(info_json_path) != ''
        ORDER BY upload_date DESC, video_id DESC
    """
    params: list[int] = []
    if limit and limit > 0:
        sql += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
    return conn.execute(sql, params).fetchall()


def collection_stats(conn: sqlite3.Connection) -> sqlite3.Row:
    return conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM videos) AS video_count,
            (SELECT COUNT(*) FROM transcripts) AS transcript_count,
            (SELECT COUNT(*) FROM video_analysis) AS analysis_count,
            (SELECT COUNT(*) FROM videos WHERE has_ko_sub = 1) AS ko_sub_count,
            (SELECT COUNT(*) FROM videos WHERE has_auto_ko_sub = 1) AS auto_ko_sub_count,
            (SELECT COUNT(*) FROM videos WHERE has_ko_sub = 1 OR has_auto_ko_sub = 1) AS any_ko_sub_count,
            (SELECT COUNT(*) FROM download_attempts WHERE stage = 'subtitle' AND status = 'failed') AS subtitle_failures,
            (SELECT COUNT(*) FROM download_attempts WHERE stage = 'subtitle' AND status = 'downloaded') AS subtitle_successes,
            (SELECT MAX(upload_date) FROM videos) AS latest_upload_date,
            (SELECT MAX(created_at) FROM download_attempts) AS latest_attempt_at
        """
    ).fetchone()


def recent_attempts(
    conn: sqlite3.Connection, *, stage: str | None = None, limit: int = 10
) -> list[sqlite3.Row]:
    if stage:
        return conn.execute(
            """
            SELECT video_id, stage, status, attempts, returncode, created_at
            FROM download_attempts
            WHERE stage = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (stage, limit),
        ).fetchall()
    return conn.execute(
        """
        SELECT video_id, stage, status, attempts, returncode, created_at
        FROM download_attempts
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def pending_video_analysis_rows(
    conn: sqlite3.Connection,
    *,
    limit: int = 0,
    overwrite: bool = False,
    video_ids: Iterable[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    oldest_first: bool = False,
) -> list[sqlite3.Row]:
    clauses = ["t.dialogue IS NOT NULL", "TRIM(t.dialogue) != ''"]
    params: list[object] = []

    if not overwrite:
        clauses.append("a.video_id IS NULL")

    if video_ids:
        ids = [video_id for video_id in video_ids if video_id]
        if ids:
            placeholders = ",".join("?" for _ in ids)
            clauses.append(f"v.video_id IN ({placeholders})")
            params.extend(ids)

    if date_from:
        clauses.append("COALESCE(v.upload_date, '') >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("COALESCE(v.upload_date, '') <= ?")
        params.append(date_to)

    order = "ASC" if oldest_first else "DESC"
    sql = f"""
        SELECT
            v.video_id,
            v.title,
            v.upload_date,
            v.view_count,
            v.like_count,
            v.source_url,
            t.dialogue,
            t.subtitle_source,
            a.summary AS existing_summary,
            a.keywords_json AS existing_keywords_json,
            a.analysis_source AS existing_analysis_source
        FROM transcripts t
        JOIN videos v ON v.video_id = t.video_id
        LEFT JOIN video_analysis a ON a.video_id = v.video_id
        WHERE {' AND '.join(clauses)}
        ORDER BY v.upload_date {order}, v.video_id ASC
    """
    if limit and limit > 0:
        sql += " LIMIT ?"
        params.append(limit)

    return conn.execute(sql, params).fetchall()
