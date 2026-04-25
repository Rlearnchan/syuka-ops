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
            channel_key TEXT NOT NULL DEFAULT 'syukaworld',
            channel_name TEXT NOT NULL DEFAULT '?덉뭅?붾뱶',
            title TEXT NOT NULL,
            upload_date TEXT,
            duration_seconds INTEGER,
            is_short INTEGER NOT NULL DEFAULT 0,
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

        CREATE TABLE IF NOT EXISTS video_ad_analysis (
            video_id TEXT PRIMARY KEY,
            ad_detected INTEGER NOT NULL DEFAULT 0,
            advertiser TEXT,
            advertiser_candidates_json TEXT,
            evidence_text TEXT,
            description_excerpt TEXT,
            confidence REAL,
            raw_json TEXT,
            analysis_source TEXT NOT NULL DEFAULT 'heuristic_description',
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
    if "channel_key" not in existing_video_columns:
        conn.execute("ALTER TABLE videos ADD COLUMN channel_key TEXT NOT NULL DEFAULT 'syukaworld'")
    if "channel_name" not in existing_video_columns:
        conn.execute("ALTER TABLE videos ADD COLUMN channel_name TEXT NOT NULL DEFAULT '?덉뭅?붾뱶'")
    if "has_auto_ko_sub" not in existing_video_columns:
        conn.execute("ALTER TABLE videos ADD COLUMN has_auto_ko_sub INTEGER NOT NULL DEFAULT 0")
    if "duration_seconds" not in existing_video_columns:
        conn.execute("ALTER TABLE videos ADD COLUMN duration_seconds INTEGER")
    if "is_short" not in existing_video_columns:
        conn.execute("ALTER TABLE videos ADD COLUMN is_short INTEGER NOT NULL DEFAULT 0")

    existing_ad_analysis_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(video_ad_analysis)").fetchall()
    }
    if "advertiser_candidates_json" not in existing_ad_analysis_columns:
        conn.execute("ALTER TABLE video_ad_analysis ADD COLUMN advertiser_candidates_json TEXT")

    existing_transcript_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(transcripts)").fetchall()
    }
    if "subtitle_source" not in existing_transcript_columns:
        conn.execute("ALTER TABLE transcripts ADD COLUMN subtitle_source TEXT NOT NULL DEFAULT 'manual'")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_upload_date ON videos(upload_date DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_channel_upload_date ON videos(channel_key, upload_date DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_has_ko_sub ON videos(has_ko_sub, upload_date DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_has_auto_ko_sub ON videos(has_auto_ko_sub, upload_date DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_video_analysis_source ON video_analysis(analysis_source, updated_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_video_ad_analysis_source ON video_ad_analysis(analysis_source, updated_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_video_ad_analysis_advertiser ON video_ad_analysis(advertiser, updated_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_attempts_video_stage ON download_attempts(video_id, stage, created_at DESC)")

    conn.commit()


def upsert_video(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO videos (
            video_id, channel_key, channel_name, title, upload_date, duration_seconds, is_short, view_count, like_count, has_ko_sub,
            has_auto_ko_sub, thumbnail_url, source_url, info_json_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            channel_key=excluded.channel_key,
            channel_name=excluded.channel_name,
            title=excluded.title,
            upload_date=excluded.upload_date,
            duration_seconds=excluded.duration_seconds,
            is_short=excluded.is_short,
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
            row.get("channel_key", "syukaworld"),
            row.get("channel_name", "?덉뭅?붾뱶"),
            row["title"],
            row.get("upload_date"),
            row.get("duration_seconds"),
            int(bool(row.get("is_short"))),
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


def upsert_video_ad_analysis(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO video_ad_analysis (
            video_id, ad_detected, advertiser, advertiser_candidates_json, evidence_text, description_excerpt,
            confidence, raw_json, analysis_source
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            ad_detected=excluded.ad_detected,
            advertiser=excluded.advertiser,
            advertiser_candidates_json=excluded.advertiser_candidates_json,
            evidence_text=excluded.evidence_text,
            description_excerpt=excluded.description_excerpt,
            confidence=excluded.confidence,
            raw_json=excluded.raw_json,
            analysis_source=excluded.analysis_source,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            row["video_id"],
            int(bool(row.get("ad_detected"))),
            row.get("advertiser"),
            row.get("advertiser_candidates_json"),
            row.get("evidence_text"),
            row.get("description_excerpt"),
            row.get("confidence"),
            row.get("raw_json"),
            row.get("analysis_source", "heuristic_description"),
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


def latest_video_date(
    conn: sqlite3.Connection,
    *,
    channel_key: str | None = None,
    is_short: bool | None = None,
) -> str | None:
    clauses: list[str] = []
    params: list[object] = []
    if channel_key:
        clauses.append("channel_key = ?")
        params.append(channel_key)
    if is_short is not None:
        clauses.append("COALESCE(is_short, 0) = ?")
        params.append(1 if is_short else 0)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    row = conn.execute(f"SELECT MAX(upload_date) AS latest_date FROM videos {where_sql}", params).fetchone()
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
    conn: sqlite3.Connection, limit: int = 10, offset: int = 0, *, channel_key: str | None = None
) -> list[sqlite3.Row]:
    params: list[object] = []
    clauses = ["COALESCE(v.is_short, 0) = 0"]
    if channel_key:
        clauses.append("v.channel_key = ?")
        params.append(channel_key)
    where_sql = f"WHERE {' AND '.join(clauses)}"
    params.extend([limit, offset])
    return conn.execute(
        f"""
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
        ORDER BY v.upload_date DESC
        LIMIT ?
        OFFSET ?
        """,
        params,
    ).fetchall()


def recent_video_count(conn: sqlite3.Connection, *, channel_key: str | None = None) -> int:
    clauses = ["COALESCE(is_short, 0) = 0"]
    params: list[object] = []
    if channel_key:
        clauses.append("channel_key = ?")
        params.append(channel_key)
    row = conn.execute(f"SELECT COUNT(*) AS c FROM videos WHERE {' AND '.join(clauses)}", params).fetchone()
    return int(row["c"] or 0)


def browse_videos(
    conn: sqlite3.Connection,
    *,
    channel_key: str | None = None,
    year: str | None = None,
    sort: str = "latest",
    limit: int = 10,
    offset: int = 0,
) -> list[sqlite3.Row]:
    clauses: list[str] = ["COALESCE(v.is_short, 0) = 0"]
    params: list[object] = []
    if channel_key:
        clauses.append("v.channel_key = ?")
        params.append(channel_key)
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


def browse_video_count(conn: sqlite3.Connection, *, channel_key: str | None = None, year: str | None = None) -> int:
    clauses: list[str] = ["COALESCE(is_short, 0) = 0"]
    params: list[object] = []
    if channel_key:
        clauses.append("channel_key = ?")
        params.append(channel_key)
    if year:
        clauses.append("COALESCE(upload_date, '') LIKE ?")
        params.append(f"{year}-%")
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    row = conn.execute(f"SELECT COUNT(*) AS c FROM videos {where_sql}", params).fetchone()
    return int(row["c"] or 0)


def browse_short_videos(
    conn: sqlite3.Connection,
    *,
    channel_key: str,
    year: str | None = None,
    sort: str = "latest",
    limit: int = 10,
    offset: int = 0,
) -> list[sqlite3.Row]:
    clauses: list[str] = ["v.channel_key = ?", "COALESCE(v.is_short, 0) = 1"]
    params: list[object] = [channel_key]
    if year:
        clauses.append("COALESCE(v.upload_date, '') LIKE ?")
        params.append(f"{year}-%")

    order_by = {
        "likes": "COALESCE(v.like_count, 0) DESC, v.upload_date DESC",
        "views": "COALESCE(v.view_count, 0) DESC, v.upload_date DESC",
        "latest": "v.upload_date DESC, v.video_id DESC",
    }.get(sort, "v.upload_date DESC, v.video_id DESC")

    sql = f"""
        SELECT
            v.*,
            0 AS segment_count,
            NULL AS subtitle_source,
            NULL AS summary,
            NULL AS keywords_json,
            NULL AS analysis_source,
            0 AS has_transcript
        FROM videos v
        WHERE {' AND '.join(clauses)}
        ORDER BY {order_by}
        LIMIT ?
        OFFSET ?
    """
    params.extend([limit, offset])
    return conn.execute(sql, params).fetchall()


def browse_short_video_count(conn: sqlite3.Connection, *, channel_key: str, year: str | None = None) -> int:
    clauses: list[str] = ["channel_key = ?", "COALESCE(is_short, 0) = 1"]
    params: list[object] = [channel_key]
    if year:
        clauses.append("COALESCE(upload_date, '') LIKE ?")
        params.append(f"{year}-%")
    row = conn.execute(f"SELECT COUNT(*) AS c FROM videos WHERE {' AND '.join(clauses)}", params).fetchone()
    return int(row["c"] or 0)


def search_videos(
    conn: sqlite3.Connection, query: str, limit: int = 10, offset: int = 0, *, channel_key: str | None = None
) -> list[sqlite3.Row]:
    like = f"%{query}%"
    compact_like = f"%{normalized_search_query(query)}%"
    params: list[object] = [
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
    ]
    channel_clause = "COALESCE(v.is_short, 0) = 0 AND ("
    if channel_key:
        channel_clause = "v.channel_key = ? AND COALESCE(v.is_short, 0) = 0 AND ("
    where_params: list[object] = []
    if channel_key:
        where_params.append(channel_key)
    return conn.execute(
        f"""
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
        WHERE {channel_clause}REPLACE(v.title, ' ', '') LIKE ?
           OR v.video_id LIKE ?
           OR REPLACE(COALESCE(a.summary, ''), ' ', '') LIKE ?
           OR REPLACE(COALESCE(a.keywords_json, ''), ' ', '') LIKE ?
           OR EXISTS (
                SELECT 1
                FROM transcripts tx
                WHERE tx.video_id = v.video_id
                  AND REPLACE(tx.dialogue, ' ', '') LIKE ?
           )
        )
        ORDER BY relevance_score DESC, v.upload_date DESC
        LIMIT ?
        OFFSET ?
        """,
        params + where_params + [compact_like, like, compact_like, compact_like, compact_like, limit, offset],
    ).fetchall()


def search_videos_count(conn: sqlite3.Connection, query: str, *, channel_key: str | None = None) -> int:
    like = f"%{query}%"
    compact_like = f"%{normalized_search_query(query)}%"
    params: list[object] = []
    channel_clause = "COALESCE(v.is_short, 0) = 0 AND ("
    if channel_key:
        channel_clause = "v.channel_key = ? AND COALESCE(v.is_short, 0) = 0 AND ("
        params.append(channel_key)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS c
        FROM videos v
        LEFT JOIN video_analysis a ON a.video_id = v.video_id
        WHERE {channel_clause}REPLACE(v.title, ' ', '') LIKE ?
           OR v.video_id LIKE ?
           OR REPLACE(COALESCE(a.summary, ''), ' ', '') LIKE ?
           OR REPLACE(COALESCE(a.keywords_json, ''), ' ', '') LIKE ?
           OR EXISTS (
                SELECT 1
                FROM transcripts tx
                WHERE tx.video_id = v.video_id
                  AND REPLACE(tx.dialogue, ' ', '') LIKE ?
           )
        )
        """,
        params + [compact_like, like, compact_like, compact_like, compact_like],
    ).fetchone()
    return int(row["c"] or 0)


def get_video(conn: sqlite3.Connection, video_id: str, *, channel_key: str | None = None) -> sqlite3.Row | None:
    where_sql = "WHERE v.video_id = ?"
    params: list[object] = [video_id]
    if channel_key:
        where_sql += " AND v.channel_key = ?"
        params.append(channel_key)
    return conn.execute(
        f"""
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
        {where_sql}
        """,
        params,
    ).fetchone()


def transcript_snippets(
    conn: sqlite3.Connection, query: str, limit: int = 5, offset: int = 0, *, channel_key: str | None = None
) -> list[sqlite3.Row]:
    like = f"%{normalized_search_query(query)}%"
    params: list[object] = []
    where_sql = "WHERE REPLACE(t.dialogue, ' ', '') LIKE ? AND COALESCE(v.is_short, 0) = 0"
    params.append(like)
    if channel_key:
        where_sql += " AND v.channel_key = ?"
        params.append(channel_key)
    params.extend([limit, offset])
    return conn.execute(
        f"""
        SELECT
            v.video_id,
            v.channel_key,
            v.channel_name,
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
        {where_sql}
        ORDER BY v.upload_date DESC
        LIMIT ?
        OFFSET ?
        """,
        params,
    ).fetchall()


def transcript_snippets_count(conn: sqlite3.Connection, query: str, *, channel_key: str | None = None) -> int:
    like = f"%{normalized_search_query(query)}%"
    params: list[object] = [like]
    where_sql = (
        "WHERE REPLACE(t.dialogue, ' ', '') LIKE ? "
        "AND EXISTS (SELECT 1 FROM videos v WHERE v.video_id = t.video_id AND COALESCE(v.is_short, 0) = 0)"
    )
    if channel_key:
        where_sql += " AND EXISTS (SELECT 1 FROM videos v WHERE v.video_id = t.video_id AND v.channel_key = ?)"
        params.append(channel_key)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS c
        FROM transcripts t
        {where_sql}
        """,
        params,
    ).fetchone()
    return int(row["c"] or 0)


def video_rows_with_info_json(
    conn: sqlite3.Connection, *, channel_key: str | None = None, limit: int = 0, offset: int = 0
) -> list[sqlite3.Row]:
    where_clauses = [
        "info_json_path IS NOT NULL",
        "TRIM(info_json_path) != ''",
        "COALESCE(is_short, 0) = 0",
    ]
    params: list[object] = []
    if channel_key:
        where_clauses.append("channel_key = ?")
        params.append(channel_key)
    sql = """
        SELECT
            video_id,
            channel_key,
            channel_name,
            title,
            upload_date,
            view_count,
            like_count,
            thumbnail_url,
            source_url,
            info_json_path
        FROM videos
        WHERE {where_sql}
        ORDER BY upload_date DESC, video_id DESC
    """
    sql = sql.format(where_sql=" AND ".join(where_clauses))
    if limit and limit > 0:
        sql += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
    return conn.execute(sql, params).fetchall()


def search_video_ad_rows(
    conn: sqlite3.Connection, query: str, limit: int = 5, offset: int = 0, *, channel_key: str | None = None
) -> list[sqlite3.Row]:
    like = f"%{query}%"
    compact_like = f"%{normalized_search_query(query)}%"
    params: list[object] = []
    clauses = ["va.ad_detected = 1", "COALESCE(v.is_short, 0) = 0"]
    if channel_key:
        clauses.append("v.channel_key = ?")
        params.append(channel_key)
    clauses.append(
        "("
        "REPLACE(COALESCE(va.advertiser, ''), ' ', '') LIKE ? "
        "OR REPLACE(COALESCE(va.advertiser_candidates_json, ''), ' ', '') LIKE ? "
        "OR REPLACE(COALESCE(va.evidence_text, ''), ' ', '') LIKE ? "
        "OR REPLACE(COALESCE(va.description_excerpt, ''), ' ', '') LIKE ? "
        "OR REPLACE(COALESCE(v.title, ''), ' ', '') LIKE ? "
        "OR COALESCE(va.raw_json, '') LIKE ?"
        ")"
    )
    params.extend([compact_like, compact_like, compact_like, compact_like, compact_like, like, limit, offset])
    return conn.execute(
        f"""
        SELECT
            v.video_id,
            v.channel_key,
            v.channel_name,
            v.title,
            v.upload_date,
            v.view_count,
            v.like_count,
            v.thumbnail_url,
            v.source_url,
            va.ad_detected,
            va.advertiser,
            va.advertiser_candidates_json,
            va.evidence_text,
            va.description_excerpt,
            va.confidence,
            va.raw_json,
            va.analysis_source
        FROM video_ad_analysis va
        JOIN videos v ON v.video_id = va.video_id
        WHERE {' AND '.join(clauses)}
        ORDER BY COALESCE(va.confidence, 0) DESC, v.upload_date DESC
        LIMIT ?
        OFFSET ?
        """,
        params,
    ).fetchall()


def search_video_ad_rows_count(conn: sqlite3.Connection, query: str, *, channel_key: str | None = None) -> int:
    like = f"%{query}%"
    compact_like = f"%{normalized_search_query(query)}%"
    params: list[object] = []
    clauses = ["va.ad_detected = 1", "COALESCE(v.is_short, 0) = 0"]
    if channel_key:
        clauses.append("v.channel_key = ?")
        params.append(channel_key)
    clauses.append(
        "("
        "REPLACE(COALESCE(va.advertiser, ''), ' ', '') LIKE ? "
        "OR REPLACE(COALESCE(va.advertiser_candidates_json, ''), ' ', '') LIKE ? "
        "OR REPLACE(COALESCE(va.evidence_text, ''), ' ', '') LIKE ? "
        "OR REPLACE(COALESCE(va.description_excerpt, ''), ' ', '') LIKE ? "
        "OR REPLACE(COALESCE(v.title, ''), ' ', '') LIKE ? "
        "OR COALESCE(va.raw_json, '') LIKE ?"
        ")"
    )
    params.extend([compact_like, compact_like, compact_like, compact_like, compact_like, like])
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS c
        FROM video_ad_analysis va
        JOIN videos v ON v.video_id = va.video_id
        WHERE {' AND '.join(clauses)}
        """,
        params,
    ).fetchone()
    return int(row["c"] or 0)


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


def pending_video_ad_analysis_rows(
    conn: sqlite3.Connection,
    *,
    limit: int = 0,
    overwrite: bool = False,
    video_ids: Iterable[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    oldest_first: bool = False,
) -> list[sqlite3.Row]:
    clauses = [
        "v.info_json_path IS NOT NULL",
        "TRIM(v.info_json_path) != ''",
        "COALESCE(v.is_short, 0) = 0",
    ]
    params: list[object] = []

    if not overwrite:
        clauses.append("va.video_id IS NULL")

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
            v.channel_key,
            v.channel_name,
            v.title,
            v.upload_date,
            v.view_count,
            v.like_count,
            v.source_url,
            v.info_json_path,
            va.ad_detected AS existing_ad_detected,
            va.advertiser AS existing_advertiser,
            va.advertiser_candidates_json AS existing_advertiser_candidates_json,
            va.analysis_source AS existing_analysis_source
        FROM videos v
        LEFT JOIN video_ad_analysis va ON va.video_id = v.video_id
        WHERE {' AND '.join(clauses)}
        ORDER BY v.upload_date {order}, v.video_id ASC
    """
    if limit and limit > 0:
        sql += " LIMIT ?"
        params.append(limit)

    return conn.execute(sql, params).fetchall()


