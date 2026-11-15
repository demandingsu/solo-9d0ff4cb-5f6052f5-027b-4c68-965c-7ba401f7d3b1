"""撤回 / 禁入清单端到端测试。"""
import csv
import io
import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.main import Settings, create_app

HEADER = ["photo_no", "camera_no", "captured_at", "species", "rel_path"]


def make_csv(rows, header=HEADER):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue().encode()


def retract_csv(photo_nos, extra_column=False):
    header = ["photo_no", "note"] if extra_column else ["photo_no"]
    return make_csv([[p, "x"] if extra_column else [p] for p in photo_nos], header)


BASE = make_csv([
    ["P0002", "CAM01", "2026-03-01T08:00:00Z", "wild_boar", "cam01/P0002.jpg"],
    ["P0003", "CAM01", "2026-03-01T08:00:30Z", "wild_boar", "cam01/P0003.jpg"],
    ["P0005", "CAM01", "2026-03-01T08:01:40Z", "roe_deer", "cam01/P0005.jpg"],
    ["P0006", "CAM01", "2026-03-01T08:02:10Z", "roe_deer", "cam01/P0006.jpg"],
    ["P0001", "CAM02", "2026-03-01T08:00:10Z", "red_fox", "cam02/P0001.jpg"],
    ["P0004", "CAM02", "2026-03-01T09:30:00Z", "red_fox", "cam02/P0004.jpg"],
])

BACKFILL = make_csv([
    ["P0007", "CAM01", "2026-03-01T08:01:05Z", "wild_boar", "cam01/P0007.jpg"],
    ["P0008", "CAM02", "2026-03-01T09:30:40Z", "red_fox", "cam02/P0008.jpg"],
    ["P0009", "CAM03", "2026-03-01T12:00:00Z", "sika_deer", "cam03/P0009.jpg"],
])


@pytest.fixture()
def env(tmp_path):
    settings = Settings(data_dir=tmp_path / "data", val_percent=20, gap_seconds=60)
    app = create_app(settings)
    client = TestClient(app, follow_redirects=True)
    return client, settings


def upload(client, data, name="batch.csv"):
    return client.post("/upload", files={"file": (name, data, "text/csv")})


def retract(client, data, name="retract.csv"):
    return client.post("/retract", files={"file": (name, data, "text/csv")})


def fetch_index(client, vid, name):
    r = client.get(f"/versions/{vid}/files/{name}")
    assert r.status_code == 200, r.text
    return list(csv.DictReader(io.StringIO(r.text)))


def all_photo_nos(client, vid):
    rows = fetch_index(client, vid, "train_index.csv") + fetch_index(client, vid, "val_index.csv")
    return [r["photo_no"] for r in rows]


def query(settings, sql, params=()):
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def test_retract_removes_photos_and_publishes_new_version(env):
    client, settings = env
    upload(client, BASE, "b1.csv")
    r = retract(client, retract_csv(["P0003"]))
    assert "已发布版本 v2" in r.text

    assert sorted(all_photo_nos(client, 2)) == ["P0001", "P0002", "P0004", "P0005", "P0006"]
    # 现役索引中彻底没有撤回照片
    assert query(settings, "SELECT photo_no FROM photos WHERE photo_no='P0003'") == []
    # 撤回版本身份在首页显示
    home = client.get("/").text
    assert "v2" in home and "禁入清单" in home


def test_retract_unseen_photo_is_banned_and_later_upload_rejected(env):
    client, settings = env
    upload(client, BASE, "b1.csv")
    r = retract(client, retract_csv(["P0099"]))
    assert "已发布版本 v2" in r.text  # 未入库编号也登记禁入并发布新版本

    late = make_csv([["P0099", "CAM05", "2026-03-01T20:00:00Z", "fox", "cam05/P0099.jpg"]])
    r = upload(client, late, "late.csv")
    assert "整批拒绝" in r.text and "禁入" in r.text
    assert len(all_photo_nos(client, 3 if False else 2)) == 6  # 现役版本仍是 v2、照片未变
    assert query(settings, "SELECT photo_no FROM photos WHERE photo_no='P0099'") == []


def test_upload_batch_with_banned_photo_is_rejected_whole(env):
    client, settings = env
    upload(client, BASE, "b1.csv")
    retract(client, retract_csv(["P0002"]))
    # 批次里既有已撤回照片，也有全新照片：整批拒绝，全新照片也不入库
    mixed = make_csv([
        ["P0002", "CAM01", "2026-03-01T08:00:00Z", "wild_boar", "cam01/P0002.jpg"],
        ["P0100", "CAM07", "2026-03-02T09:00:00Z", "badger", "cam07/P0100.jpg"],
    ])
    r = upload(client, mixed, "mixed.csv")
    assert "整批拒绝" in r.text
    assert query(settings, "SELECT photo_no FROM photos WHERE photo_no='P0100'") == []
    assert "P0100" not in all_photo_nos(client, 2)


def test_duplicate_retraction_creates_no_new_version(env):
    client, settings = env
    upload(client, BASE, "b1.csv")
    retract(client, retract_csv(["P0002"]), "r1.csv")
    versions_before = len(query(settings, "SELECT id FROM versions"))
    r = retract(client, retract_csv(["P0002"]), "r2.csv")
    assert "未产生新版本" in r.text
    assert len(query(settings, "SELECT id FROM versions")) == versions_before
    # 批次记为撤回类型 noop
    rows = query(settings, "SELECT status, kind FROM batches ORDER BY id DESC LIMIT 1")
    assert (rows[0]["status"], rows[0]["kind"]) == ("noop", "retract")


def test_retraction_csv_ignores_other_columns_and_normalizes_whitespace(env):
    client, settings = env
    upload(client, BASE, "b1.csv")
    data = b"photo_no,note,whatever\n P0003 ,x,1\nP0004,y,2\n"
    r = retract(client, data)
    assert "已发布版本 v2" in r.text
    nos = all_photo_nos(client, 2)
    assert "P0003" not in nos and "P0004" not in nos


def test_retraction_csv_validation_rejects_whole_batch(env):
    client, settings = env
    upload(client, BASE, "b1.csv")
    bad = b"camera_no\nCAM01\n"
    r = retract(client, bad)
    assert "拒绝" in r.text
    assert query(settings, "SELECT COUNT(*) AS n FROM retractions")[0]["n"] == 0
    assert len(all_photo_nos(client, 1)) == 6
    rows = query(settings, "SELECT status, kind FROM batches ORDER BY id DESC LIMIT 1")
    assert (rows[0]["status"], rows[0]["kind"]) == ("rejected", "retract")


def test_old_version_with_retracted_photo_is_not_downloadable_but_clean_old_version_is(env):
    client, settings = env
    upload(client, BASE, "b1.csv")  # v1 含 6 张
    upload(client, BACKFILL, "b2.csv")  # v2 含 9 张
    # 撤回一张只在 v2 出现的照片（P0009 是 batch2 新增）
    retract(client, retract_csv(["P0009"]))  # v3

    # v2 含 P0009 -> 两份索引都 403
    for name in ("train_index.csv", "val_index.csv"):
        assert client.get(f"/versions/2/files/{name}").status_code == 403
    # v1 不含任何撤回照片 -> 仍可下载，内容不变
    v1_train = client.get("/versions/1/files/train_index.csv")
    v1_val = client.get("/versions/1/files/val_index.csv")
    assert v1_train.status_code == 200 and v1_val.status_code == 200
    assert len(list(csv.DictReader(io.StringIO(v1_train.text)))) + len(
        list(csv.DictReader(io.StringIO(v1_val.text)))
    ) == 6
    # v3 现役版本可下载，且不含 P0009
    assert "P0009" not in all_photo_nos(client, 3)
    # 首页对 v2 标记下载禁用
    home = client.get("/").text
    assert "含撤回照片" in home


def test_old_version_containing_retracted_photo_blocked_after_later_upload(env):
    client, settings = env
    upload(client, BASE, "b1.csv")  # v1: 含 P0002
    upload(client, BACKFILL, "b2.csv")  # v2
    retract(client, retract_csv(["P0002"]))  # v3：P0002 在 v1、v2 中均存在
    for vid in (1, 2):
        for name in ("train_index.csv", "val_index.csv"):
            assert client.get(f"/versions/{vid}/files/{name}").status_code == 403
    assert client.get("/versions/3/files/train_index.csv").status_code == 200
    assert "P0002" not in all_photo_nos(client, 3)


def test_retract_recomputes_groups(env):
    client, settings = env
    upload(client, BASE, "b1.csv")
    upload(client, BACKFILL, "b2.csv")  # CAM01 合并为组 P0002（P0002,P0003,P0005,P0006,P0007）
    # 撤回桥接照片 P0007 -> CAM01 应重新分裂为两组
    retract(client, retract_csv(["P0007"]))
    rows = [r for r in (fetch_index(client, 3, "train_index.csv") + fetch_index(client, 3, "val_index.csv"))
            if r["camera_no"] == "CAM01"]
    assert {r["group_id"] for r in rows} == {"P0002", "P0005"}
    assert sorted(r["photo_no"] for r in rows) == ["P0002", "P0003", "P0005", "P0006"]


def test_retract_write_failure_leaves_no_partial_state(env, monkeypatch):
    client, settings = env
    upload(client, BASE, "b1.csv")
    v1_train = client.get("/versions/1/files/train_index.csv").text

    import app.main as main

    def boom(*args, **kwargs):
        raise RuntimeError("磁盘写入失败（模拟）")

    monkeypatch.setattr(main, "write_version_files", boom)
    r = retract(client, retract_csv(["P0002"]))
    assert "撤回失败" in r.text and "未发生部分撤回" in r.text

    # 禁入登记、删除、版本指针全部回滚
    assert query(settings, "SELECT COUNT(*) AS n FROM retractions")[0]["n"] == 0
    assert query(settings, "SELECT COUNT(*) AS n FROM photos")[0]["n"] == 6
    assert query(settings, "SELECT COUNT(*) AS n FROM versions")[0]["n"] == 1
    assert client.get("/versions/1/files/train_index.csv").text == v1_train
    # 失败批次留审计记录
    rows = query(settings, "SELECT status, kind FROM batches ORDER BY id DESC LIMIT 1")
    assert (rows[0]["status"], rows[0]["kind"]) == ("failed", "retract")

    # 故障恢复后重新撤回可成功
    monkeypatch.undo()
    r2 = retract(client, retract_csv(["P0002"]))
    assert "已发布版本 v2" in r2.text
    assert query(settings, "SELECT COUNT(*) AS n FROM photos")[0]["n"] == 5


def test_retract_db_write_failure_rolls_back_files_and_state(env, monkeypatch):
    """索引文件已落盘后写库失败：目录清理、事务回滚，重试可复用同一版本号。"""
    client, settings = env
    upload(client, BASE, "b1.csv")

    import app.main as main

    def fail_members(conn, vid, photo_nos):
        raise RuntimeError("写库失败（模拟）")

    # 注入点在 rename 之后、COMMIT 之前：模拟文件已发布但写库失败。
    monkeypatch.setattr(main, "_record_version_members", fail_members)
    r = retract(client, retract_csv(["P0002"]))
    assert "撤回失败" in r.text
    assert query(settings, "SELECT COUNT(*) AS n FROM retractions")[0]["n"] == 0
    assert query(settings, "SELECT COUNT(*) AS n FROM photos")[0]["n"] == 6
    # 半成品版本目录已清理
    assert not (settings.versions_dir / "v2").exists()
    assert not list(settings.versions_dir.glob(".tmp-*"))

    monkeypatch.undo()
    r2 = retract(client, retract_csv(["P0002"]))
    assert "已发布版本 v2" in r2.text  # 回滚释放了版本号，v2 目录可重新创建
    assert (settings.versions_dir / "v2" / "train_index.csv").is_file()


def test_concurrent_upload_and_retraction_converge_to_committed_state(env):
    """上传与撤回并发：无论谁先提交，最终禁入、现役索引与下载权限一致。"""
    client, settings = env
    upload(client, BASE, "b1.csv")
    target = make_csv([["P0400", "CAM12", "2026-03-04T09:00:00Z", "boar", "c12/P0400.jpg"]])

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=2) as pool:
        fu = pool.submit(upload, client, target, "u.csv")
        fr = pool.submit(retract, client, retract_csv(["P0400"]), "r.csv")
        ru, rr = fu.result(), fr.result()
    assert ru.status_code == 200 and rr.status_code == 200

    active = int(query(settings, "SELECT value FROM meta WHERE key='active_version'")[0]["value"])
    assert query(settings, "SELECT COUNT(*) AS n FROM retractions WHERE photo_no='P0400'")[0]["n"] == 1
    assert "P0400" not in all_photo_nos(client, active)
    # 所有含 P0400 的历史版本都被禁用，不含的仍可下载。
    # 两种合法交错：上传先提交 -> 撤回随后发新版本并禁用那个版本；
    # 撤回先提交 -> 上传整批拒绝，任何版本都不含 P0400。
    blocked = {r["version_id"] for r in query(settings, "SELECT version_id FROM version_photos WHERE photo_no='P0400'")}
    for vid in {r["id"] for r in query(settings, "SELECT id FROM versions")}:
        status = client.get(f"/versions/{vid}/files/train_index.csv").status_code
        assert status == (403 if vid in blocked else 200)
    assert query(settings, "SELECT COUNT(*) AS n FROM photos WHERE photo_no='P0400'")[0]["n"] == 0
    # 再补传必定被拒
    r = upload(client, target, "again.csv")
    assert "整批拒绝" in r.text


def test_download_initiated_after_retraction_never_serves_retracted_photo(env):
    """撤回提交后新发起的下载不能拿到已撤回照片（含旧版本）。"""
    client, settings = env
    upload(client, BASE, "b1.csv")
    upload(client, BACKFILL, "b2.csv")
    # 撤回前 v2 可下载且含 P0009
    assert "P0009" in client.get("/versions/2/files/train_index.csv").text + \
        client.get("/versions/2/files/val_index.csv").text

    retract(client, retract_csv(["P0009"]))

    for vid in (2, 3):
        for name in ("train_index.csv", "val_index.csv"):
            resp = client.get(f"/versions/{vid}/files/{name}")
            if vid == 3:
                assert resp.status_code == 200
                assert "P0009" not in resp.text
            else:
                assert resp.status_code == 403
                # 只返回错误说明，不返回索引内容（CSV 表头/数据行）
                assert "photo_no,camera_no" not in resp.text
                assert "content-type" in resp.headers
                assert "text/csv" not in resp.headers["content-type"]
    # 响应不会被缓存复用
    assert client.get("/versions/3/files/train_index.csv").headers.get("cache-control") == "no-store"


def test_upload_and_retraction_interleaved_stay_consistent(env):
    """交错提交后：禁入记录、现役索引、旧版下载权限对应同一已提交状态。"""
    client, settings = env
    upload(client, BASE, "b1.csv")
    # 交错：先上传含 P0300 的批次，再撤回 P0300（若顺序相反则上传会被拒绝）
    upload(client, make_csv([["P0300", "CAM11", "2026-03-03T10:00:00Z", "boar", "c11/P0300.jpg"]]), "u.csv")
    retract(client, retract_csv(["P0300"]), "r.csv")

    active = int(query(settings, "SELECT value FROM meta WHERE key='active_version'")[0]["value"])
    assert "P0300" not in all_photo_nos(client, active)
    assert query(settings, "SELECT COUNT(*) AS n FROM retractions WHERE photo_no='P0300'")[0]["n"] == 1
    # 含 P0300 的版本（v2）下载被拒，v1 不受影响
    assert client.get("/versions/2/files/train_index.csv").status_code == 403
    assert client.get("/versions/1/files/train_index.csv").status_code == 200

    # 再次补传 P0300 被整批拒绝，现役版本不变
    r = upload(client, make_csv([["P0300", "CAM11", "2026-03-03T10:00:00Z", "boar", "c11/P0300.jpg"]]), "late.csv")
    assert "整批拒绝" in r.text
    assert int(query(settings, "SELECT value FROM meta WHERE key='active_version'")[0]["value"]) == active


def test_retract_all_photos_publishes_empty_version(env):
    client, settings = env
    upload(client, BASE, "b1.csv")
    all_nos = [r["photo_no"] for r in (fetch_index(client, 1, "train_index.csv") + fetch_index(client, 1, "val_index.csv"))]
    r = retract(client, retract_csv(all_nos))
    assert "已发布版本 v2" in r.text
    assert all_photo_nos(client, 2) == []
    # 空版本详情页仍可打开
    assert client.get("/versions/2").status_code == 200
    assert query(settings, "SELECT COUNT(*) AS n FROM photos")[0]["n"] == 0


def test_startup_cleans_stale_version_dirs(tmp_path):
    """启动时删除无 versions 行的孤儿目录与 .tmp-*，保留已登记版本。"""
    import app.main as main

    settings = Settings(data_dir=tmp_path / "data")
    app = main.create_app(settings)
    client = TestClient(app, follow_redirects=True)
    upload(client, BASE, "b1.csv")

    orphan = settings.versions_dir / "v99"
    orphan.mkdir()
    (orphan / "train_index.csv").write_text("stale", encoding="utf-8")
    tmp = settings.versions_dir / ".tmp-v2-1-deadbeef"
    tmp.mkdir()

    # 模拟重启
    main.create_app(settings)
    assert not orphan.exists()
    assert not tmp.exists()
    assert (settings.versions_dir / "v1" / "train_index.csv").is_file()


def test_migration_backfills_version_members_for_existing_db(tmp_path):
    """旧库（无 version_photos）启动时按不可变索引文件回填成员表。"""
    import app.main as main

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    versions_dir = data_dir / "versions" / "v1"
    versions_dir.mkdir(parents=True)
    (versions_dir / "train_index.csv").write_text(
        "photo_no,camera_no,captured_at,species,rel_path,group_id\n"
        "P0001,CAM02,2026-03-01T08:00:10Z,red_fox,cam02/P0001.jpg,P0001\n",
        encoding="utf-8",
    )
    (versions_dir / "val_index.csv").write_text(
        "photo_no,camera_no,captured_at,species,rel_path,group_id\n"
        "P0002,CAM01,2026-03-01T08:00:00Z,wild_boar,cam01/P0002.jpg,P0002\n",
        encoding="utf-8",
    )
    # 模拟旧版 schema：batches 无 kind、无 version_photos/retractions 表
    conn = sqlite3.connect(data_dir / "app.db")
    conn.executescript("""
        CREATE TABLE batches (id INTEGER PRIMARY KEY AUTOINCREMENT, uploaded_at TEXT NOT NULL,
            filename TEXT NOT NULL, rows_total INTEGER NOT NULL, rows_new INTEGER NOT NULL,
            rows_duplicate INTEGER NOT NULL, status TEXT NOT NULL, message TEXT, version_id INTEGER);
        CREATE TABLE versions (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
            photo_count INTEGER NOT NULL, group_count INTEGER NOT NULL, train_count INTEGER NOT NULL,
            val_count INTEGER NOT NULL, train_path TEXT NOT NULL DEFAULT '', val_path TEXT NOT NULL DEFAULT '');
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO versions(created_at, photo_count, group_count, train_count, val_count, train_path, val_path)
            VALUES ('2026-03-01T00:00:00Z', 2, 2, 1, 1, 'versions/v1/train_index.csv', 'versions/v1/val_index.csv');
        INSERT INTO meta(key, value) VALUES ('active_version', '1');
    """)
    conn.commit()
    conn.close()

    settings = Settings(data_dir=data_dir)
    app = main.create_app(settings)
    client = TestClient(app)

    assert query(settings, "SELECT photo_no FROM version_photos ORDER BY photo_no")[0]["photo_no"] == "P0001"
    assert len(query(settings, "SELECT photo_no FROM version_photos")) == 2
    assert query(settings, "PRAGMA table_info(batches)")[0]["name"] == "id"
    assert any(r["name"] == "kind" for r in query(settings, "PRAGMA table_info(batches)"))

    # 撤回 P0001 后，回填出的 v1 成员关系使 v1 下载被拒
    r = client.post("/retract", files={"file": ("r.csv", retract_csv(["P0001"]), "text/csv")},
                    follow_redirects=True)
    assert "已发布版本" in r.text
    assert client.get("/versions/1/files/train_index.csv").status_code == 403
