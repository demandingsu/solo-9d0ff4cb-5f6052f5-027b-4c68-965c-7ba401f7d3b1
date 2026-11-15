"""野生动物红外相机照片索引服务（FastAPI + SQLite）。

核心保证
--------
* 每个含新照片的批次发布一个版本：train/val 两份索引 + 版本号。
* 版本不可变：索引文件写入 versions/v{N}/ 后永不修改，旧链接永远下载原内容。
* 发布是原子的：照片入库、索引文件落盘（tmp 目录 + os.rename）、
  versions 行与 active_version 指针在同一个 BEGIN IMMEDIATE 事务中提交；
  任一步失败整体回滚，现役版本不变。多进程/多请求交错上传由 SQLite
  写锁串行化，页面与两份下载始终对应同一完整版本。
* 幂等：同号同内容重投 -> 批次记为 noop，不产生新版本；
  同号异内容 -> 整批拒绝（rejected），库内不产生任何变化。
* 撤回：上传只含 photo_no 的 CSV，在一个写事务内登记禁入清单、删除在库照片、
  基于剩余照片重新归组并发布新版本。禁入清单是永久的：之后任何包含禁入编号的
  上传批次整批拒绝，避免迟到补传让已撤回照片重新进入索引。
* 撤回原子性：禁入登记、照片删除、索引文件落盘、versions 行、版本成员表与
  active_version 指针同事务提交；任一步失败整体回滚，不会出现"部分撤回"。
* 历史版本：索引文件保持不可变；只要版本包含任一已撤回照片，train/val 两份
  下载一律拒绝。未包含撤回照片的旧版本仍可正常下载。下载与撤回交错时，
  版本成员、禁入记录与下载权限读取同一 SQLite 快照。
"""

from __future__ import annotations

import csv
import io
import os
import shutil
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse

from .grouping import Photo, assign_split, compute_groups

REQUIRED_COLUMNS = ("photo_no", "camera_no", "captured_at", "species", "rel_path")
INDEX_COLUMNS = ("photo_no", "camera_no", "captured_at", "species", "rel_path", "group_id")
MAX_UPLOAD_BYTES = 100 * 1024 * 1024

STATUS_LABELS = {
    "applied": "已发布",
    "noop": "无变化",
    "rejected": "已拒绝",
    "failed": "发布失败",
}

RETRACT_PLAIN_COLUMNS = ("photo_no",)


# ---------------------------------------------------------------- 配置

@dataclass(frozen=True)
class Settings:
    data_dir: Path
    val_percent: int = 20
    gap_seconds: int = 60
    split_salt: str = "wildlife-trap-v1"
    samples_dir: Path | None = None

    @property
    def db_path(self) -> Path:
        return self.data_dir / "app.db"

    @property
    def versions_dir(self) -> Path:
        return self.data_dir / "versions"

    @classmethod
    def from_env(cls) -> "Settings":
        samples = os.environ.get("SAMPLES_DIR", "samples")
        samples_dir = Path(samples).resolve() if samples else None
        return cls(
            data_dir=Path(os.environ.get("DATA_DIR", "./data")).resolve(),
            val_percent=int(os.environ.get("VAL_PERCENT", "20")),
            gap_seconds=int(os.environ.get("GAP_SECONDS", "60")),
            split_salt=os.environ.get("SPLIT_SALT", "wildlife-trap-v1"),
            samples_dir=samples_dir if samples_dir and samples_dir.is_dir() else None,
        )


# ---------------------------------------------------------------- 时间

def utcnow_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ts_str(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_timestamp(raw: str) -> datetime:
    s = raw.strip()
    if not s:
        raise ValueError("空时间戳")
    dt = datetime.fromisoformat(s)  # 支持 "...Z" 与偏移；naive 视为 UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def parse_stored_ts(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------- 数据库

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uploaded_at TEXT NOT NULL,
    filename TEXT NOT NULL,
    rows_total INTEGER NOT NULL,
    rows_new INTEGER NOT NULL,
    rows_duplicate INTEGER NOT NULL,
    status TEXT NOT NULL,              -- applied | noop | rejected | failed
    message TEXT,
    version_id INTEGER,
    kind TEXT NOT NULL DEFAULT 'upload' -- upload | retract
);
CREATE TABLE IF NOT EXISTS photos (
    photo_no TEXT PRIMARY KEY,
    camera_no TEXT NOT NULL,
    captured_at TEXT NOT NULL,         -- ISO8601 UTC, 秒精度
    species TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    batch_id INTEGER NOT NULL REFERENCES batches(id)
);
CREATE TABLE IF NOT EXISTS versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    photo_count INTEGER NOT NULL,
    group_count INTEGER NOT NULL,
    train_count INTEGER NOT NULL,
    val_count INTEGER NOT NULL,
    train_path TEXT NOT NULL DEFAULT '',
    val_path TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS version_photos (
    version_id INTEGER NOT NULL REFERENCES versions(id),
    photo_no TEXT NOT NULL,
    PRIMARY KEY (version_id, photo_no)
);
CREATE TABLE IF NOT EXISTS retractions (
    photo_no TEXT PRIMARY KEY,          -- 永久禁入清单（大小写敏感，与 photo_no 一致）
    retracted_at TEXT NOT NULL,
    batch_id INTEGER NOT NULL REFERENCES batches(id),
    was_in_db INTEGER NOT NULL DEFAULT 0  -- 撤回时是否在库（被从现役索引移除）
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_version_photos_photo ON version_photos(photo_no);
"""


# 旧库升级：CREATE TABLE IF NOT EXISTS 不会补列，这里做幂等迁移。
MIGRATIONS = (
    ("batches", "kind", "ALTER TABLE batches ADD COLUMN kind TEXT NOT NULL DEFAULT 'upload'"),
)



def connect(settings: Settings) -> sqlite3.Connection:
    conn = sqlite3.connect(settings.db_path, timeout=15, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_storage(settings: Settings) -> None:
    settings.versions_dir.mkdir(parents=True, exist_ok=True)
    conn = connect(settings)
    try:
        conn.executescript(SCHEMA)
        # 迁移在写锁内复查再执行，避免多进程同时启动时重复 ALTER。
        conn.execute("BEGIN IMMEDIATE")
        try:
            for table, column, ddl in MIGRATIONS:
                cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
                if column not in cols:
                    conn.execute(ddl)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        # 旧库第一次升级时 version_photos 为空：按不可变索引文件回填成员表，
        # 让历史版本的"是否含撤回照片"可判定。只回填尚未有成员记录的版本。
        _backfill_version_photos(conn, settings)
        _cleanup_stale_version_dirs(conn, settings)
    finally:
        conn.close()


def _cleanup_stale_version_dirs(conn: sqlite3.Connection, settings: Settings) -> None:
    """删除上次进程崩溃遗留的半成品/孤儿目录。

    在 BEGIN IMMEDIATE 写锁内执行：并发启动或正在发布的其他进程持有写锁时，
    这里会等待其提交或回滚，因此不会误删别人刚 rename、尚未提交的版本目录。
    有 versions 行的 v{N} 目录保留；没有对应行的 v{N} 目录和所有 .tmp-* 删除。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        known = {f"v{r['id']}" for r in conn.execute("SELECT id FROM versions")}
        for d in settings.versions_dir.iterdir():
            if not d.is_dir():
                continue
            if d.name.startswith(".tmp-") or d.name not in known:
                shutil.rmtree(d, ignore_errors=True)
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise


def _backfill_version_photos(conn: sqlite3.Connection, settings: Settings) -> None:
    versions = conn.execute(
        "SELECT v.id, v.train_path, v.val_path FROM versions v"
        " WHERE NOT EXISTS (SELECT 1 FROM version_photos vp WHERE vp.version_id = v.id)"
    ).fetchall()
    if not versions:
        return
    conn.execute("BEGIN IMMEDIATE")
    try:
        for v in versions:
            members: set[str] = set()
            for col in ("train_path", "val_path"):
                if not v[col]:
                    continue
                path = settings.data_dir / v[col]
                if not path.is_file():
                    continue
                with open(path, newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        if row.get("photo_no"):
                            members.add(row["photo_no"])
            conn.executemany(
                "INSERT OR IGNORE INTO version_photos(version_id, photo_no) VALUES (?,?)",
                [(v["id"], p) for p in sorted(members)],
            )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise



def get_active_version(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT value FROM meta WHERE key='active_version'").fetchone()
    return int(row["value"]) if row else None


def version_download_row(conn: sqlite3.Connection, vid: int) -> sqlite3.Row | None:
    """读取版本及其下载禁用状态（单条语句 = 同一 WAL 快照）。

    blocked=1 表示该版本索引里至少有一张照片已进入禁入清单。
    """
    return conn.execute(
        "SELECT v.*, EXISTS("
        "  SELECT 1 FROM version_photos vp JOIN retractions r ON r.photo_no = vp.photo_no"
        "  WHERE vp.version_id = v.id"
        ") AS blocked FROM versions v WHERE v.id=?",
        (vid,),
    ).fetchone()


def retracted_in_version(conn: sqlite3.Connection, vid: int) -> list[str]:
    return [
        r["photo_no"]
        for r in conn.execute(
            "SELECT DISTINCT vp.photo_no FROM version_photos vp"
            " JOIN retractions r ON r.photo_no = vp.photo_no"
            " WHERE vp.version_id=? ORDER BY vp.photo_no",
            (vid,),
        )
    ]


@contextmanager
def read_snapshot(conn: sqlite3.Connection):
    """在一个显式读事务内执行多条查询，保证它们来自同一已提交 WAL 快照。"""
    conn.execute("BEGIN DEFERRED")
    try:
        yield conn
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    conn.execute("COMMIT")



# ---------------------------------------------------------------- CSV 解析

@dataclass(frozen=True)
class CsvRow:
    photo_no: str
    camera_no: str
    captured_at: datetime
    species: str
    rel_path: str

    def content_tuple(self) -> tuple[str, str, str, str]:
        return (self.camera_no, ts_str(self.captured_at), self.species, self.rel_path)


def parse_csv(
    data: bytes, required_columns: tuple[str, ...] = REQUIRED_COLUMNS
) -> tuple[list[CsvRow], list[str]]:
    """解析并校验整份 CSV。任何行出错都返回错误列表（调用方整批拒绝）。"""
    errors: list[str] = []
    rows: list[CsvRow] = []
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return [], ["文件不是有效的 UTF-8 编码"]

    reader = csv.reader(io.StringIO(text))
    header = None
    for raw in reader:
        if raw and any(c.strip() for c in raw):
            header = raw
            break
    if header is None:
        return [], ["CSV 为空或缺少表头"]

    cols = {name.strip(): i for i, name in enumerate(header)}
    missing = [c for c in required_columns if c not in cols]
    if missing:
        return [], [f"缺少必需列: {', '.join(missing)}（需要: {', '.join(required_columns)}）"]

    seen: dict[str, CsvRow] = {}
    for lineno, raw in enumerate(reader, start=2):
        if not raw or all(not c.strip() for c in raw):
            continue

        def cell(name: str) -> str:
            i = cols[name]
            return raw[i].strip() if i < len(raw) else ""

        photo_no, camera_no = cell("photo_no"), cell("camera_no")
        ts_raw, species, rel_path = cell("captured_at"), cell("species"), cell("rel_path")

        row_errs: list[str] = []
        if not photo_no:
            row_errs.append("photo_no 为空")
        if not camera_no:
            row_errs.append("camera_no 为空")
        if not species:
            row_errs.append("species 为空")
        if not rel_path:
            row_errs.append("rel_path 为空")
        captured_at = None
        try:
            captured_at = parse_timestamp(ts_raw)
        except ValueError:
            row_errs.append(f"captured_at 无法解析: {ts_raw!r}（需 ISO8601，如 2026-03-01T08:00:00Z）")

        if row_errs:
            errors.append(f"第{lineno}行: " + "；".join(row_errs))
            continue

        row = CsvRow(photo_no, camera_no, captured_at, species, rel_path)
        if photo_no in seen:
            if seen[photo_no].content_tuple() != row.content_tuple():
                errors.append(f"第{lineno}行: photo_no {photo_no!r} 在文件内重复且内容不一致")
            continue  # 文件内完全重复的行视为同一照片
        seen[photo_no] = row
        rows.append(row)

    if not rows and not errors:
        errors.append("CSV 没有任何数据行")
    return rows, errors


def parse_retraction_csv(data: bytes) -> tuple[list[str], list[str]]:
    """解析撤回清单：只需 photo_no 一列，其他列一律忽略。

    返回 (去重后的照片号列表, 错误列表)。任何错误 -> 调用方整批拒绝。
    """
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return [], ["文件不是有效的 UTF-8 编码"]

    reader = csv.reader(io.StringIO(text))
    header = None
    for raw in reader:
        if raw and any(c.strip() for c in raw):
            header = raw
            break
    if header is None:
        return [], ["CSV 为空或缺少表头"]

    cols = {name.strip(): i for i, name in enumerate(header)}
    if "photo_no" not in cols:
        return [], ["缺少必需列: photo_no（撤回文件只需一列 photo_no）"]

    photo_nos: list[str] = []
    seen: set[str] = set()
    errors: list[str] = []
    idx = cols["photo_no"]
    for lineno, raw in enumerate(reader, start=2):
        if not raw or all(not c.strip() for c in raw):
            continue
        photo_no = raw[idx].strip() if idx < len(raw) else ""
        if not photo_no:
            errors.append(f"第{lineno}行: photo_no 为空")
            continue
        if photo_no in seen:
            continue  # 文件内重复编号只处理一次
        seen.add(photo_no)
        photo_nos.append(photo_no)

    if not photo_nos and not errors:
        errors.append("CSV 没有任何数据行")
    return photo_nos, errors


# ---------------------------------------------------------------- 索引文件

def _write_index_csv(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(INDEX_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)


def write_version_files(versions_dir: Path, vid: int, train_rows: list[dict], val_rows: list[dict]) -> Path:
    """把两份索引先写进临时目录，再原子 rename 成 versions/v{vid}/。

    rename 在同文件系统上是原子的：读者要么看不到该版本目录，
    要么看到包含两份完整索引的目录。
    """
    target = versions_dir / f"v{vid}"
    tmp = versions_dir / f".tmp-v{vid}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    try:
        _write_index_csv(tmp / "train_index.csv", train_rows)
        _write_index_csv(tmp / "val_index.csv", val_rows)
        if target.exists():
            raise FileExistsError(f"版本目录已存在: {target}")
        os.rename(tmp, target)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    return target


# ---------------------------------------------------------------- 批次处理

def _record_batch(conn, filename, rows_total, rows_new, rows_dup, status, message, version_id=None, kind="upload") -> int:
    cur = conn.execute(
        "INSERT INTO batches(uploaded_at, filename, rows_total, rows_new, rows_duplicate, status, message, version_id, kind)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        (utcnow_str(), filename, rows_total, rows_new, rows_dup, status, message, version_id, kind),
    )
    return int(cur.lastrowid)


def record_rejected_batch(
    settings: Settings, filename: str, rows_total: int, message: str, kind: str = "upload"
) -> int:
    """CSV 解析/校验失败：不触碰照片数据，仅留审计记录。"""
    conn = connect(settings)
    try:
        conn.execute("BEGIN IMMEDIATE")
        bid = _record_batch(conn, filename, rows_total, 0, 0, "rejected", message, kind=kind)
        conn.execute("COMMIT")
        return bid
    finally:
        conn.close()


def _load_photos(conn: sqlite3.Connection) -> list[Photo]:
    return [
        Photo(p["photo_no"], p["camera_no"], parse_stored_ts(p["captured_at"]), p["species"], p["rel_path"])
        for p in conn.execute("SELECT photo_no, camera_no, captured_at, species, rel_path FROM photos")
    ]


def _build_index_rows(settings: Settings, photos: list[Photo]) -> tuple[list[dict], list[dict], list]:
    """按当前配置归组划分，返回 (train_rows, val_rows, groups)。"""
    groups = compute_groups(photos, settings.gap_seconds)
    train_rows: list[dict] = []
    val_rows: list[dict] = []
    for g in groups:
        target = val_rows if assign_split(g.group_id, settings.val_percent, settings.split_salt) == "val" else train_rows
        for p in g.photos:
            target.append({
                "photo_no": p.photo_no,
                "camera_no": p.camera_no,
                "captured_at": ts_str(p.captured_at),
                "species": p.species,
                "rel_path": p.rel_path,
                "group_id": g.group_id,
            })
    key = lambda r: (r["camera_no"], r["captured_at"], r["photo_no"])
    train_rows.sort(key=key)
    val_rows.sort(key=key)
    return train_rows, val_rows, groups


def _publish_version(
    conn: sqlite3.Connection,
    settings: Settings,
    train_rows: list[dict],
    val_rows: list[dict],
    group_count: int,
    photos: list[Photo],
) -> tuple[int, Path]:
    """插入 versions + version_photos，落盘两份索引，切换 active_version。

    调用方必须已在 BEGIN IMMEDIATE 事务中；异常时由调用方回滚并清理返回目录。
    文件先写入 .tmp-* 再 rename，任何读者都不会看到半套索引。
    versions 行先插入（AUTOINCREMENT 在事务内已占位），文件落盘后再补齐路径与
    成员；即使落盘失败，回滚也会撤销占位，版本号不会被外部观察到。
    """
    cur = conn.execute(
        "INSERT INTO versions(created_at, photo_count, group_count, train_count, val_count) VALUES (?,?,?,?,?)",
        (utcnow_str(), len(photos), group_count, len(train_rows), len(val_rows)),
    )
    vid = int(cur.lastrowid)

    published_dir: Path | None = None
    try:
        published_dir = write_version_files(settings.versions_dir, vid, train_rows, val_rows)
        rel = Path("versions") / f"v{vid}"
        conn.execute(
            "UPDATE versions SET train_path=?, val_path=? WHERE id=?",
            (str(rel / "train_index.csv"), str(rel / "val_index.csv"), vid),
        )
        # 版本成员表与索引文件同事务写入：历史版本是否含撤回照片由此判定。
        _record_version_members(conn, vid, [p.photo_no for p in photos])
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('active_version', ?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(vid),),
        )
    except BaseException:
        # rename 之后、提交之前任何一步失败：此刻仍持有写锁，先删掉本事务的版本目录
        # 再让调用方回滚，避免留下孤儿目录（v{vid} 会随回滚被下一个版本复用）。
        if published_dir is not None:
            shutil.rmtree(published_dir, ignore_errors=True)
        raise
    return vid, published_dir


def _record_version_members(conn: sqlite3.Connection, vid: int, photo_nos: list[str]) -> None:
    conn.executemany(
        "INSERT INTO version_photos(version_id, photo_no) VALUES (?,?)",
        [(vid, p) for p in photo_nos],
    )


def process_batch(settings: Settings, filename: str, rows: list[CsvRow]) -> tuple[str, int, str]:
    """在单个写事务内完成：禁入校验 -> 冲突检测 -> 入库 -> 归组划分 -> 索引落盘 -> 切换现役版本。"""
    conn = connect(settings)
    published_dir: Path | None = None
    try:
        conn.execute("BEGIN IMMEDIATE")

        # 禁入清单与冲突检测在同一个写事务快照内读取：与撤回交错时要么看到
        # 撤回前状态（可入库，随后撤回会再清理），要么看到撤回后状态（整批拒绝）。
        retracted = {r["photo_no"] for r in conn.execute("SELECT photo_no FROM retractions")}
        blocked = sorted({row.photo_no for row in rows if row.photo_no in retracted})
        if blocked:
            shown = ", ".join(blocked[:20])
            msg = (
                f"整批拒绝：{len(blocked)} 个照片号在禁入清单中，不能重新入库"
                f"（{shown}{' …' if len(blocked) > 20 else ''}）"
            )
            bid = _record_batch(conn, filename, len(rows), 0, 0, "rejected", msg)
            conn.execute("COMMIT")
            return "rejected", bid, msg

        existing = {
            r["photo_no"]: r
            for r in conn.execute("SELECT photo_no, camera_no, captured_at, species, rel_path FROM photos")
        }
        new_rows: list[CsvRow] = []
        conflicts: list[str] = []
        dup = 0
        for row in rows:
            ex = existing.get(row.photo_no)
            if ex is None:
                new_rows.append(row)
            elif (ex["camera_no"], ex["captured_at"], ex["species"], ex["rel_path"]) == row.content_tuple():
                dup += 1
            else:
                conflicts.append(row.photo_no)

        if conflicts:
            shown = ", ".join(sorted(conflicts)[:20])
            msg = f"整批拒绝：{len(conflicts)} 个照片号已存在但内容不一致（{shown}{' …' if len(conflicts) > 20 else ''}）"
            bid = _record_batch(conn, filename, len(rows), 0, dup, "rejected", msg)
            conn.execute("COMMIT")
            return "rejected", bid, msg

        if not new_rows:
            msg = f"批内 {dup} 张照片全部已存在且内容一致，未产生新版本"
            bid = _record_batch(conn, filename, len(rows), 0, dup, "noop", msg)
            conn.execute("COMMIT")
            return "noop", bid, msg

        batch_id = _record_batch(conn, filename, len(rows), len(new_rows), dup, "applied", None)
        conn.executemany(
            "INSERT INTO photos(photo_no, camera_no, captured_at, species, rel_path, batch_id) VALUES (?,?,?,?,?,?)",
            [(r.photo_no, r.camera_no, ts_str(r.captured_at), r.species, r.rel_path, batch_id) for r in new_rows],
        )

        # 基于全量照片重新归组（补传可能合并旧组）
        all_photos = _load_photos(conn)
        train_rows, val_rows, groups = _build_index_rows(settings, all_photos)

        # 索引文件落盘（tmp + rename）+ 版本行/成员/指针。失败 -> 整体回滚，现役版本不变。
        vid, published_dir = _publish_version(conn, settings, train_rows, val_rows, len(groups), all_photos)
        conn.execute("UPDATE batches SET version_id=? WHERE id=?", (vid, batch_id))
        msg = f"已发布版本 v{vid}：新增 {len(new_rows)} 张（重复 {dup} 张），共 {len(all_photos)} 张 / {len(groups)} 组，训练 {len(train_rows)} / 验证 {len(val_rows)}"
        conn.execute("UPDATE batches SET message=? WHERE id=?", (msg, batch_id))
        conn.execute("COMMIT")
        return "applied", batch_id, msg

    except BaseException as exc:
        # 先清理半成品版本目录，再回滚：清理时仍持有写锁，其他写者无法复用同一 v{vid}
        # 目录名（AUTOINCREMENT 序列随回滚恢复），不会误删别人刚发布的版本。
        if published_dir is not None:
            shutil.rmtree(published_dir, ignore_errors=True)
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        try:  # 失败审计（独立小事务，失败不影响主流程）
            conn.execute("BEGIN IMMEDIATE")
            _record_batch(conn, filename, len(rows), 0, 0, "failed", f"发布失败，现役版本未变：{exc}"[:500])
            conn.execute("COMMIT")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


def process_retraction(settings: Settings, filename: str, photo_nos: list[str]) -> tuple[str, int, str]:
    """在单个写事务内完成撤回：登记禁入 -> 删除在库照片 -> 重新归组 -> 发布新版本。

    * 任何编号（在库或未入库）都永久进入 retractions 禁入清单。
    * 重复撤回（没有新编号、也没有可删除照片）记 noop，不产生新版本。
    * 文件落盘失败或写库失败 -> 整体回滚：禁入登记、删除、版本指针全部不生效。
    """
    conn = connect(settings)
    published_dir: Path | None = None
    try:
        conn.execute("BEGIN IMMEDIATE")

        already = {r["photo_no"] for r in conn.execute("SELECT photo_no FROM retractions")}
        in_db = {r["photo_no"] for r in conn.execute("SELECT photo_no FROM photos")}
        new_bans = [p for p in photo_nos if p not in already]
        to_delete = [p for p in photo_nos if p in in_db]

        if not new_bans and not to_delete:
            msg = f"重复撤回：{len(photo_nos)} 个编号均已在禁入清单且不在现役索引中，未产生新版本"
            bid = _record_batch(conn, filename, len(photo_nos), 0, len(photo_nos), "noop", msg, kind="retract")
            conn.execute("COMMIT")
            return "noop", bid, msg

        bid = _record_batch(
            conn, filename, len(photo_nos), len(new_bans), len(photo_nos) - len(new_bans), "applied", None, kind="retract"
        )
        now = utcnow_str()
        conn.executemany(
            "INSERT INTO retractions(photo_no, retracted_at, batch_id, was_in_db) VALUES (?,?,?,?)",
            [(p, now, bid, 1 if p in in_db else 0) for p in new_bans],
        )
        if to_delete:
            conn.executemany("DELETE FROM photos WHERE photo_no=?", [(p,) for p in to_delete])

        remaining = _load_photos(conn)
        train_rows, val_rows, groups = _build_index_rows(settings, remaining)

        vid, published_dir = _publish_version(conn, settings, train_rows, val_rows, len(groups), remaining)
        conn.execute("UPDATE batches SET version_id=? WHERE id=?", (vid, bid))
        msg = (
            f"已发布版本 v{vid}：撤回 {len(photo_nos)} 个编号（现役索引删除 {len(to_delete)} 张，"
            f"新增禁入 {len(new_bans)} 个），共 {len(remaining)} 张 / {len(groups)} 组，"
            f"训练 {len(train_rows)} / 验证 {len(val_rows)}"
        )
        conn.execute("UPDATE batches SET message=? WHERE id=?", (msg, bid))
        conn.execute("COMMIT")
        return "applied", bid, msg

    except BaseException as exc:
        # 先清理半成品版本目录再回滚：清理时仍持有写锁，其他写者无法复用同一 v{vid}
        # 目录名（AUTOINCREMENT 序列随回滚恢复），不会误删别人刚发布的版本。
        if published_dir is not None:
            shutil.rmtree(published_dir, ignore_errors=True)
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        try:
            conn.execute("BEGIN IMMEDIATE")
            _record_batch(
                conn, filename, len(photo_nos), 0, 0, "failed", f"撤回失败，禁入清单与现役版本均未变：{exc}"[:500], kind="retract"
            )
            conn.execute("COMMIT")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()



# ---------------------------------------------------------------- 页面渲染

PAGE_CSS = """
body{font-family:-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial,"Noto Sans SC",sans-serif;margin:0;background:#f5f6f8;color:#222}
header{background:#2f5233;color:#fff;padding:14px 24px}
header h1{margin:0;font-size:20px}
header a{color:#fff;text-decoration:none}
main{max-width:1080px;margin:24px auto;padding:0 16px}
.card{background:#fff;border:1px solid #ddd;border-radius:8px;padding:16px 20px;margin-bottom:20px}
table{border-collapse:collapse;width:100%;font-size:14px}
th,td{border:1px solid #ddd;padding:6px 10px;text-align:left;vertical-align:top}
th{background:#eef3ee}
.badge{display:inline-block;background:#2f5233;color:#fff;border-radius:4px;padding:1px 8px;font-size:12px}
.flash-ok{background:#e6f4e6;border:1px solid #7ab87a;color:#1d5c1d}
.flash-err{background:#fde8e8;border:1px solid #e08a8a;color:#8a1f1f}
.flash-info{background:#e8f0fe;border:1px solid #8aa8e0;color:#1f3d8a}
.flash{border-radius:6px;padding:10px 14px;margin-bottom:16px;font-size:14px}
code,pre{background:#f0f0f0;border-radius:4px}
code{padding:1px 5px}
pre{padding:10px;overflow:auto;font-size:13px}
.btn{display:inline-block;background:#2f5233;color:#fff;border:none;border-radius:5px;padding:7px 16px;font-size:14px;cursor:pointer;text-decoration:none}
.btn.secondary{background:#5a6b5d}
.muted{color:#666;font-size:13px}
ol.steps li{margin-bottom:6px}
"""


def page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)} · 红外相机照片索引</title>
<style>{PAGE_CSS}</style></head>
<body><header><h1><a href="/">野生动物红外相机照片索引</a></h1></header>
<main>{body}</main></body></html>"""


def flash_html(msg: str, kind: str) -> str:
    if not msg:
        return ""
    kind = kind if kind in ("ok", "err", "info") else "info"
    return f'<div class="flash flash-{kind}">{escape(msg)}</div>'


def home_page(settings: Settings, msg: str, kind: str) -> str:
    conn = connect(settings)
    try:
        # 同一读快照内取现役版本、版本表、批次表、版本成员与禁入清单：
        # 页面展示的下载权限与禁入记录不会出现"半撤回"状态。
        with read_snapshot(conn):
            active_id = get_active_version(conn)
            versions = conn.execute("SELECT * FROM versions ORDER BY id DESC").fetchall()
            batches = conn.execute("SELECT * FROM batches ORDER BY id DESC LIMIT 50").fetchall()
            blocked_version_ids = {
                r["version_id"]
                for r in conn.execute(
                    "SELECT DISTINCT vp.version_id FROM version_photos vp"
                    " JOIN retractions r ON r.photo_no = vp.photo_no"
                )
            }
            retracted = conn.execute(
                "SELECT r.photo_no, r.retracted_at, b.id AS batch_id FROM retractions r"
                " LEFT JOIN batches b ON b.id = r.batch_id ORDER BY r.photo_no"
            ).fetchall()
    finally:
        conn.close()

    def download_links(vid: int) -> str:
        if vid in blocked_version_ids:
            return '<span class="muted">已禁用（含撤回照片）</span>'
        return (
            f'<a href="/versions/{vid}/files/train_index.csv">train</a> · '
            f'<a href="/versions/{vid}/files/val_index.csv">val</a>'
        )

    # 现役版本横幅
    if active_id is not None:
        active = next((v for v in versions if v["id"] == active_id), None)
        if active_id in blocked_version_ids:  # 不应出现：撤回同时会发新版本
            active_links = '<span class="muted">下载已禁用（含撤回照片）</span>'
        else:
            active_links = (
                f'<a class="btn" href="/versions/{active_id}/files/train_index.csv">下载训练索引</a> '
                f'<a class="btn" href="/versions/{active_id}/files/val_index.csv">下载验证索引</a>'
            )
        banner = (
            f'<div class="card"><h2 style="margin-top:0">现役版本 <span class="badge">v{active_id}</span></h2>'
            f"<p>照片 {active['photo_count']} 张 · 出没事件组 {active['group_count']} 个 · "
            f"训练 {active['train_count']} / 验证 {active['val_count']} · 发布于 {active['created_at']} UTC</p>"
            f'<p>{active_links} '
            f'<a class="btn secondary" href="/versions/{active_id}">查看分组</a></p></div>'
        )
    else:
        banner = '<div class="card"><p>尚未发布任何版本，请上传第一批 CSV。</p></div>'

    # 上传表单
    upload = f"""
<div class="card"><h2 style="margin-top:0">上传照片 CSV 并发布新版本</h2>
<form action="/upload" method="post" enctype="multipart/form-data">
<p><input type="file" name="file" accept=".csv,text/csv" required>
<button class="btn" type="submit">上传并发布</button></p>
</form>
<p class="muted">必需列：<code>{'</code> <code>'.join(REQUIRED_COLUMNS)}</code>；
时间为 UTC（ISO8601，如 <code>2026-03-01T08:00:00Z</code>）。
同一相机相邻 ≤{settings.gap_seconds} 秒的照片传递归组；按组稳定划分（验证集比例 {settings.val_percent}%）。
同号同内容重投不产生新版本；同号异内容整批拒绝；含禁入照片号的批次整批拒绝。</p></div>"""

    # 撤回表单
    retract_form = """
<div class="card"><h2 style="margin-top:0">撤回照片（禁入清单）</h2>
<form action="/retract" method="post" enctype="multipart/form-data">
<p><input type="file" name="file" accept=".csv,text/csv" required>
<button class="btn" type="submit">撤回并发布新版本</button></p>
</form>
<p class="muted">上传只含 <code>photo_no</code> 一列的 CSV（其他列忽略）。在库照片从现役索引移除并
重新归组；未入库编号同样登记禁入，之后任何包含这些编号的上传批次整批拒绝。
重复撤回不产生新版本。含撤回照片的历史版本将禁止下载两份索引。</p></div>"""

    retracted_html = ""
    if retracted:
        rrows = "".join(
            f"<tr><td>{escape(r['photo_no'])}</td><td>{r['retracted_at']}</td>"
            f"<td><a href=\"/batches/{r['batch_id']}\">#{r['batch_id']}</a></td></tr>"
            if r["batch_id"] else f"<tr><td>{escape(r['photo_no'])}</td><td>{r['retracted_at']}</td><td>—</td></tr>"
            for r in retracted
        )
        retracted_html = (
            f'<div class="card"><h2 style="margin-top:0">禁入清单（{len(retracted)}）</h2>'
            "<p class='muted'>禁入为永久记录：这些照片号不会重新进入任何版本。</p>"
            "<table><tr><th>照片号</th><th>登记时间(UTC)</th><th>撤回批次</th></tr>"
            + rrows + "</table></div>"
        )

    # 样例
    samples_html = ""
    if settings.samples_dir:
        files = sorted(p.name for p in settings.samples_dir.glob("*.csv"))
        if files:
            links = " ".join(f'<li><a href="/samples/{escape(n)}">{escape(n)}</a></li>' for n in files)
            samples_html = f"""
<div class="card"><h2 style="margin-top:0">跨批补传演示（浏览器可操作）</h2>
<ol class="steps">
<li>下载样例 CSV：<ul>{links}</ul></li>
<li>上传 <code>batch1_base.csv</code> → 发布 v1（CAM01 的两段出没因间隔 70 秒分为两组）。</li>
<li>上传 <code>batch2_backfill.csv</code> → 发布 v2：补传的 P0007（08:01:05）与前后两组各相隔 35 秒，
把旧两组合并成一组，全库重新归组；P0008 加入 CAM02 旧组。</li>
<li>上传 <code>batch3_duplicate.csv</code>（与 batch2 完全相同）→ 不产生新版本。</li>
<li>上传 <code>batch4_conflict.csv</code>（P0001 内容被改动）→ 整批拒绝，现役版本保持 v2。</li>
<li>在「撤回照片」表单上传 <code>batch5_retract.csv</code>（P0007、P0009）→ 发布 v3：两份索引移除这两张，
CAM01 因桥接照片 P0007 被撤回而重新分裂为两组；v1 不含撤回照片仍可下载，v2 含 P0007/P0009，两份索引下载被禁用。</li>
<li>再上传一次 <code>batch5_retract.csv</code> → 重复撤回，不产生新版本。</li>
<li>尝试补传含 P0009 的批次（可自行编写 CSV）→ 整批拒绝。</li>
<li>随时打开 v1 的下载链接，内容保持最初发布时的原样。</li>
</ol></div>"""

    # 版本表
    if versions:
        vrows = []
        for v in versions:
            active_badge = ' <span class="badge">现役</span>' if v["id"] == active_id else ""
            if v["id"] in blocked_version_ids:
                active_badge += ' <span class="badge" style="background:#8a1f1f">含撤回照片，下载禁用</span>'
            vrows.append(
                f"<tr><td><a href=\"/versions/{v['id']}\">v{v['id']}</a>{active_badge}</td>"
                f"<td>{v['created_at']}</td><td>{v['photo_count']}</td><td>{v['group_count']}</td>"
                f"<td>{v['train_count']}</td><td>{v['val_count']}</td>"
                f"<td>{download_links(v['id'])}</td></tr>"
            )
        versions_html = (
            '<div class="card"><h2 style="margin-top:0">版本历史（未受撤回影响的旧版本内容与下载不变）</h2>'
            "<table><tr><th>版本</th><th>发布时间(UTC)</th><th>照片数</th><th>组数</th>"
            "<th>训练</th><th>验证</th><th>索引下载</th></tr>" + "".join(vrows) + "</table></div>"
        )
    else:
        versions_html = ""

    # 批次表
    if batches:
        brows = []
        for b in batches:
            label = STATUS_LABELS.get(b["status"], b["status"])
            kind_label = "撤回" if b["kind"] == "retract" else "上传"
            ver = f'<a href="/versions/{b["version_id"]}">v{b["version_id"]}</a>' if b["version_id"] else "—"
            brows.append(
                f'<tr><td><a href="/batches/{b["id"]}">#{b["id"]}</a></td><td>{b["uploaded_at"]}</td>'
                f"<td>{kind_label}</td><td>{escape(b['filename'])}</td><td>{label}</td>"
                f"<td>{b['rows_total']}</td><td>{b['rows_new']}</td><td>{b['rows_duplicate']}</td><td>{ver}</td></tr>"
            )
        batches_html = (
            '<div class="card"><h2 style="margin-top:0">批次记录</h2>'
            "<table><tr><th>批次</th><th>时间(UTC)</th><th>类型</th><th>文件名</th><th>状态</th>"
            "<th>总行数</th><th>新增/禁入</th><th>重复/已禁入</th><th>产生版本</th></tr>" + "".join(brows) + "</table></div>"
        )
    else:
        batches_html = ""

    return page(
        "首页",
        flash_html(msg, kind) + banner + upload + retract_form + retracted_html + samples_html + versions_html + batches_html,
    )


def version_detail_page(settings: Settings, vid: int) -> str:
    conn = connect(settings)
    try:
        with read_snapshot(conn):
            v = version_download_row(conn, vid)
            active_id = get_active_version(conn)
            blocked_photos = retracted_in_version(conn, vid) if v is not None else []
    finally:
        conn.close()
    if v is None:
        raise HTTPException(404, "版本不存在")

    groups: dict[str, dict] = {}
    for split, col in (("train", "train_path"), ("val", "val_path")):
        path = settings.data_dir / v[col]
        if not path.is_file():
            raise HTTPException(500, f"索引文件缺失: {v[col]}")
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                g = groups.setdefault(row["group_id"], {"split": split, "rows": []})
                g["rows"].append(row)

    grows = []
    for gid in sorted(groups, key=lambda g: (min(r["captured_at"] for r in groups[g]["rows"]), g)):
        g = groups[gid]
        times = sorted(r["captured_at"] for r in g["rows"])
        members = ", ".join(r["photo_no"] for r in sorted(g["rows"], key=lambda r: (r["captured_at"], r["photo_no"])))
        split_label = "验证" if g["split"] == "val" else "训练"
        grows.append(
            f"<tr><td>{escape(gid)}</td><td>{escape(g['rows'][0]['camera_no'])}</td>"
            f"<td>{times[0]}</td><td>{times[-1]}</td><td>{len(g['rows'])}</td>"
            f"<td>{split_label}</td><td>{escape(members)}</td></tr>"
        )

    active_badge = ' <span class="badge">现役</span>' if vid == active_id else ""
    if v["blocked"]:
        shown = ", ".join(blocked_photos[:20]) + (" …" if len(blocked_photos) > 20 else "")
        access_html = (
            '<div class="flash flash-err">该版本包含已撤回照片（'
            f"{escape(shown)}），两份索引下载已禁用。索引内容仍以原始文件留存以备审计。</div>"
        )
    else:
        access_html = (
            f'<p><a class="btn" href="/versions/{vid}/files/train_index.csv">下载训练索引</a>'
            f'<a class="btn" href="/versions/{vid}/files/val_index.csv">下载验证索引</a>'
            f'<a class="btn secondary" href="/">返回首页</a></p>'
        )
    body = f"""
<div class="card"><h2 style="margin-top:0">版本 v{vid}{active_badge}</h2>
<p>发布于 {v['created_at']} UTC · 照片 {v['photo_count']} 张 · 组 {v['group_count']} 个 ·
训练 {v['train_count']} / 验证 {v['val_count']}</p>
{access_html}</div>
<div class="card"><h2 style="margin-top:0">出没事件分组（group_id = 组内最早照片号）</h2>
<table><tr><th>group_id</th><th>相机</th><th>开始(UTC)</th><th>结束(UTC)</th><th>张数</th><th>划分</th><th>成员照片</th></tr>
{''.join(grows)}</table></div>"""
    return page(f"版本 v{vid}", body)


def batch_detail_page(settings: Settings, bid: int) -> str:
    conn = connect(settings)
    try:
        b = conn.execute("SELECT * FROM batches WHERE id=?", (bid,)).fetchone()
        retracted_here = []
        if b is not None and b["kind"] == "retract":
            retracted_here = conn.execute(
                "SELECT photo_no, retracted_at, was_in_db FROM retractions"
                " WHERE batch_id=? ORDER BY photo_no",
                (bid,),
            ).fetchall()
    finally:
        conn.close()
    if b is None:
        raise HTTPException(404, "批次不存在")
    label = STATUS_LABELS.get(b["status"], b["status"])
    kind_label = "撤回" if b["kind"] == "retract" else "上传"
    ver = f'<a href="/versions/{b["version_id"]}">v{b["version_id"]}</a>' if b["version_id"] else "—"
    photos = []
    if b["status"] == "applied" and b["kind"] == "upload":
        conn = connect(settings)
        try:
            photos = conn.execute(
                "SELECT photo_no, camera_no, captured_at, species, rel_path FROM photos WHERE batch_id=? ORDER BY camera_no, captured_at, photo_no",
                (bid,),
            ).fetchall()
        finally:
            conn.close()
    prows = "".join(
        f"<tr><td>{escape(p['photo_no'])}</td><td>{escape(p['camera_no'])}</td><td>{p['captured_at']}</td>"
        f"<td>{escape(p['species'])}</td><td>{escape(p['rel_path'])}</td></tr>"
        for p in photos
    )
    photos_html = (
        f'<div class="card"><h2 style="margin-top:0">本批入库照片（{len(photos)}）</h2>'
        "<table><tr><th>照片号</th><th>相机</th><th>拍摄时间(UTC)</th><th>物种</th><th>原图相对路径</th></tr>"
        f"{prows}</table></div>"
        if photos else ""
    )
    rrows = "".join(
        f"<tr><td>{escape(r['photo_no'])}</td><td>{r['retracted_at']}</td>"
        f"<td>{'已从现役索引移除' if r['was_in_db'] else '当时未入库，仅登记禁入'}</td></tr>"
        for r in retracted_here
    )
    retracted_html = (
        f'<div class="card"><h2 style="margin-top:0">本批登记禁入（{len(retracted_here)}）</h2>'
        "<table><tr><th>照片号</th><th>登记时间(UTC)</th><th>当时处理</th></tr>"
        f"{rrows}</table></div>"
        if retracted_here else ""
    )
    body = f"""
<div class="card"><h2 style="margin-top:0">批次 #{b['id']}</h2>
<table>
<tr><th>时间(UTC)</th><td>{b['uploaded_at']}</td></tr>
<tr><th>类型</th><td>{kind_label}</td></tr>
<tr><th>文件名</th><td>{escape(b['filename'])}</td></tr>
<tr><th>状态</th><td>{label}</td></tr>
<tr><th>总行数 / 新增(禁入) / 重复(已禁入)</th><td>{b['rows_total']} / {b['rows_new']} / {b['rows_duplicate']}</td></tr>
<tr><th>产生版本</th><td>{ver}</td></tr>
<tr><th>详情</th><td><pre>{escape(b['message'] or '—')}</pre></td></tr>
</table>
<p><a class="btn secondary" href="/">返回首页</a></p></div>
{retracted_html}{photos_html}"""
    return page(f"批次 #{bid}", body)


# ---------------------------------------------------------------- 应用

def create_app(settings: Settings) -> FastAPI:
    init_storage(settings)
    app = FastAPI(title="野生动物红外相机照片索引", docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/", response_class=HTMLResponse)
    def home(msg: str = "", kind: str = "info") -> str:
        return home_page(settings, msg, kind)

    @app.post("/upload")
    def upload(file: UploadFile = File(...)):
        filename = file.filename or "upload.csv"
        data = file.file.read(MAX_UPLOAD_BYTES + 1)
        if len(data) > MAX_UPLOAD_BYTES:
            return RedirectResponse("/?" + urlencode({"kind": "err", "msg": "文件超过 100MB 限制"}), 303)

        rows, errors = parse_csv(data)
        if errors:
            bid = record_rejected_batch(settings, filename, len(rows), "整批拒绝：\n" + "\n".join(errors[:50]))
            return RedirectResponse(
                "/?" + urlencode({"kind": "err", "msg": f"批次 #{bid} 被拒绝：{errors[0]}" + (" …" if len(errors) > 1 else "")}),
                303,
            )

        try:
            status, batch_id, message = process_batch(settings, filename, rows)
        except Exception as exc:  # 发布失败：事务已回滚，现役版本不变
            return RedirectResponse(
                "/?" + urlencode({"kind": "err", "msg": f"发布失败，现役版本未变：{exc}"}), 303
            )
        kind = {"applied": "ok", "noop": "info", "rejected": "err"}[status]
        return RedirectResponse("/?" + urlencode({"kind": kind, "msg": message}), 303)

    @app.post("/retract")
    def retract(file: UploadFile = File(...)):
        filename = file.filename or "retract.csv"
        data = file.file.read(MAX_UPLOAD_BYTES + 1)
        if len(data) > MAX_UPLOAD_BYTES:
            return RedirectResponse("/?" + urlencode({"kind": "err", "msg": "文件超过 100MB 限制"}), 303)

        photo_nos, errors = parse_retraction_csv(data)
        if errors:
            bid = record_rejected_batch(
                settings, filename, len(photo_nos), "整批拒绝：\n" + "\n".join(errors[:50]), kind="retract"
            )
            return RedirectResponse(
                "/?" + urlencode({"kind": "err", "msg": f"撤回批次 #{bid} 被拒绝：{errors[0]}" + (" …" if len(errors) > 1 else "")}),
                303,
            )

        try:
            status, batch_id, message = process_retraction(settings, filename, photo_nos)
        except Exception as exc:  # 撤回失败：事务已回滚，禁入清单与现役版本均不变
            return RedirectResponse(
                "/?" + urlencode({"kind": "err", "msg": f"撤回失败，未发生部分撤回：{exc}"}), 303
            )
        kind = {"applied": "ok", "noop": "info"}[status]
        return RedirectResponse("/?" + urlencode({"kind": kind, "msg": message}), 303)

    @app.get("/versions/{vid}", response_class=HTMLResponse)
    def version_detail(vid: int) -> str:
        return version_detail_page(settings, vid)

    @app.get("/versions/{vid}/files/{name}")
    def version_file(vid: int, name: str):
        if name not in ("train_index.csv", "val_index.csv"):
            raise HTTPException(404, "未知文件")
        conn = connect(settings)
        try:
            # 版本存在性、版本成员与禁入清单在同一读快照中判定：撤回事务提交前
            # 发起的下载服务旧快照；提交后发起的下载一律 403。
            with read_snapshot(conn):
                v = version_download_row(conn, vid)
                blocked = retracted_in_version(conn, vid) if v is not None and v["blocked"] else []
        finally:
            conn.close()
        if v is None:
            raise HTTPException(404, "版本不存在")
        if v["blocked"]:
            raise HTTPException(
                403,
                f"版本 v{vid} 包含已撤回照片（{', '.join(blocked[:10])}"
                f"{' …' if len(blocked) > 10 else ''}），两份索引均禁止下载",
            )
        rel = v["train_path"] if name == "train_index.csv" else v["val_path"]
        path = settings.data_dir / rel
        if not path.is_file():
            raise HTTPException(404, "索引文件缺失")
        return FileResponse(
            path,
            media_type="text/csv",
            filename=f"v{vid}_{name}",
            headers={"Cache-Control": "no-store"},
        )

    @app.get("/batches/{bid}", response_class=HTMLResponse)
    def batch_detail(bid: int) -> str:
        return batch_detail_page(settings, bid)

    if settings.samples_dir:
        samples_root = settings.samples_dir

        @app.get("/samples/{name}")
        def sample_file(name: str):
            path = (samples_root / name).resolve()
            if not path.is_file() or samples_root not in path.parents:
                raise HTTPException(404, "样例不存在")
            return FileResponse(path, media_type="text/csv", filename=name)

    @app.get("/healthz")
    def healthz() -> dict:
        return {"ok": True}

    return app


app = create_app(Settings.from_env())
