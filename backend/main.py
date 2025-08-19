import asyncio
import json
import os
import sqlite3
import secrets
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, DefaultDict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from backend.serial_manager import SerialManager, SerialConfig

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "logs.sqlite3"
CONFIG_PATH = DATA_DIR / "serial_config.json"
STATIC_DIR = BASE_DIR / "static"

DATA_DIR.mkdir(parents=True, exist_ok=True)
STATIC_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="XLRemoteSerial - Lite")

app.add_middleware(
	CORSMiddleware,
	allow_origins=["*"],
	allow_credentials=True,
	allow_methods=["*"],
	allow_headers=["*"],
)

# 连接管理（本地默认设备与按设备码的房间）
active_clients: Set[WebSocket] = set()
code_to_clients: Dict[str, Set[WebSocket]] = {}

# 队列改为携带设备码
line_queue: asyncio.Queue[Tuple[str, str]] = asyncio.Queue(maxsize=10000)
serial_manager: Optional[SerialManager] = None
_db_lock = asyncio.Lock()
fts_enabled: bool = False
app.state.device: Dict[str, Any] = {}


def _enqueue_line_for_device(device_code: str, text: str) -> None:
	try:
		line_queue.put_nowait((device_code, text))
	except asyncio.QueueFull:
		try:
			line_queue.get_nowait()
		except Exception:
			pass
		try:
			line_queue.put_nowait((device_code, text))
		except Exception:
			pass


@contextmanager
def db_conn():
	conn = sqlite3.connect(DB_PATH)
	try:
		conn.execute("PRAGMA journal_mode=WAL;")
		conn.execute("PRAGMA synchronous=NORMAL;")
		yield conn
	finally:
		conn.close()


def init_db() -> None:
	with db_conn() as conn:
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS log (
				id INTEGER PRIMARY KEY,
				ts TEXT NOT NULL,
				level TEXT,
				content TEXT NOT NULL
			);
			"""
		)
		# 设备表
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS device (
				id INTEGER PRIMARY KEY,
				device_code TEXT UNIQUE NOT NULL,
				created_at TEXT NOT NULL
			);
			"""
		)
		conn.execute("CREATE INDEX IF NOT EXISTS idx_log_ts ON log(ts);")
		conn.commit()
	# 补充列迁移：log.device_id
	migrate_add_device_id_column()
	init_fts()


def migrate_add_device_id_column() -> None:
	try:
		with db_conn() as conn:
			cols = [r[1] for r in conn.execute("PRAGMA table_info(log)").fetchall()]
			if "device_id" not in cols:
				conn.execute("ALTER TABLE log ADD COLUMN device_id INTEGER;")
				conn.execute("CREATE INDEX IF NOT EXISTS idx_log_device_ts ON log(device_id, ts);")
				conn.commit()
	except Exception:
		pass


def init_fts() -> None:
	global fts_enabled
	try:
		with db_conn() as conn:
			conn.execute(
				"""
				CREATE VIRTUAL TABLE IF NOT EXISTS log_fts USING fts5(
					content,
					content='log',
					content_rowid='id'
				);
				"""
			)
			conn.executescript(
				"""
				CREATE TRIGGER IF NOT EXISTS log_ai AFTER INSERT ON log BEGIN
				  INSERT INTO log_fts(rowid, content) VALUES (new.id, new.content);
				END;
				CREATE TRIGGER IF NOT EXISTS log_ad AFTER DELETE ON log BEGIN
				  INSERT INTO log_fts(log_fts, rowid, content) VALUES('delete', old.id, old.content);
				END;
				CREATE TRIGGER IF NOT EXISTS log_au AFTER UPDATE ON log BEGIN
				  INSERT INTO log_fts(log_fts, rowid, content) VALUES('delete', old.id, old.content);
				  INSERT INTO log_fts(rowid, content) VALUES (new.id, new.content);
				END;
				"""
			)
			conn.commit()
		fts_enabled = True
	except Exception:
		fts_enabled = False


def _generate_device_code(length: int = 8) -> str:
	alphabet = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"
	return "".join(secrets.choice(alphabet) for _ in range(length))


def ensure_device() -> Dict[str, Any]:
	with db_conn() as conn:
		row = conn.execute("SELECT id, device_code, created_at FROM device LIMIT 1").fetchone()
		if row:
			return {"id": row[0], "device_code": row[1], "created_at": row[2]}
		code = _generate_device_code(8)
		created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
		cur = conn.execute("INSERT INTO device (device_code, created_at) VALUES (?, ?)", (code, created_at))
		conn.commit()
		dev_id = cur.lastrowid
		return {"id": dev_id, "device_code": code, "created_at": created_at}


def migrate_fill_device_id(device_id: int) -> None:
	with db_conn() as conn:
		conn.execute("UPDATE log SET device_id = ? WHERE device_id IS NULL OR device_id = ''", (device_id,))
		conn.commit()


def insert_log(content: str, level: str = "info", device_id: Optional[int] = None) -> None:
	ts = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
	with db_conn() as conn:
		if device_id is None:
			conn.execute("INSERT INTO log (ts, level, content) VALUES (?, ?, ?);", (ts, level, content))
		else:
			conn.execute("INSERT INTO log (ts, level, content, device_id) VALUES (?, ?, ?, ?);", (ts, level, content, device_id))
		conn.commit()


def _build_fts_query(q: str) -> str:
	tokens = [t for t in q.strip().split() if t]
	if not tokens:
		return ""
	safe = []
	for t in tokens:
		t = t.replace('"', '""')
		safe.append(f'"{t}*"')
	return " AND ".join(safe)


def query_logs_with_total(limit: int = 200, offset: int = 0, q: Optional[str] = None, level: Optional[str] = None,
						  time_from: Optional[str] = None, time_to: Optional[str] = None, device_code: Optional[str] = None) -> Tuple[List[Dict[str, Any]], int]:
	use_fts = bool(q) and fts_enabled
	params_list: List[Any] = []
	params_count: List[Any] = []

	if use_fts:
		fts_q = _build_fts_query(q or "")
		base_from = " FROM log l JOIN log_fts f ON f.rowid = l.id WHERE log_fts MATCH ?"
		params_list.append(fts_q)
		params_count.append(fts_q)
	else:
		base_from = " FROM log l WHERE 1=1"

	if device_code:
		base_from += " AND l.device_id = (SELECT id FROM device WHERE device_code = ?)"
		params_list.append(device_code)
		params_count.append(device_code)

	if q and not use_fts:
		base_from += " AND l.content LIKE ?"
		like_q = f"%{q}%"
		params_list.append(like_q)
		params_count.append(like_q)

	if level:
		base_from += " AND l.level = ?"
		params_list.append(level)
		params_count.append(level)

	if time_from:
		base_from += " AND l.ts >= ?"
		params_list.append(time_from)
		params_count.append(time_from)

	if time_to:
		base_from += " AND l.ts <= ?"
		params_list.append(time_to)
		params_count.append(time_to)

	sql_list = "SELECT l.ts, l.level, l.content" + base_from + " ORDER BY l.ts DESC LIMIT ? OFFSET ?"
	sql_count = "SELECT COUNT(1)" + base_from

	params_list_ext = list(params_list) + [limit, offset]

	with db_conn() as conn:
		total = conn.execute(sql_count, params_count).fetchone()[0]
		rows = conn.execute(sql_list, params_list_ext).fetchall()
	items = [{"ts": r[0], "level": r[1], "content": r[2]} for r in rows]
	return items, total


async def broadcaster() -> None:
	while True:
		device_code, text = await line_queue.get()
		dev = app.state.device
		device_id = dev.get("id") if isinstance(dev, dict) else None
		insert_log(text, device_id=device_id)
		dead_global: List[WebSocket] = []
		for ws in list(active_clients):
			try:
				await ws.send_text(text)
			except Exception:
				dead_global.append(ws)
		for ws in dead_global:
			try:
				active_clients.discard(ws)
				await ws.close()
			except Exception:
				pass
		# 按设备码广播
		clients = code_to_clients.get(device_code) or set()
		dead_room: List[WebSocket] = []
		for ws in list(clients):
			try:
				await ws.send_text(text)
			except Exception:
				dead_room.append(ws)
		for ws in dead_room:
			try:
				clients.discard(ws)
				await ws.close()
			except Exception:
				pass


@app.on_event("startup")
async def on_startup() -> None:
	global serial_manager
	init_db()
	# 确保设备码，并回填历史日志的 device_id
	app.state.device = ensure_device()
	try:
		migrate_fill_device_id(int(app.state.device.get("id")))
	except Exception:
		pass
	loop = asyncio.get_running_loop()
	serial_manager = SerialManager(lambda text: loop.call_soon_threadsafe(_enqueue_line_for_device, app.state.device["device_code"], text))
	if CONFIG_PATH.exists():
		try:
			cfg = json.loads(CONFIG_PATH.read_text("utf-8"))
			if "port" in cfg:
				serial_manager.start(
					SerialConfig(
						port=cfg["port"],
						baudrate=cfg.get("baudrate", 115200),
						bytesize=cfg.get("bytesize", 8),
						stopbits=cfg.get("stopbits", 1),
						parity=cfg.get("parity", "N"),
						timeout=cfg.get("timeout", 0.1),
						encoding=cfg.get("encoding", "utf-8"),
					)
				)
		except Exception:
			pass
	app.state.broadcast_task = asyncio.create_task(broadcaster())


@app.on_event("shutdown")
async def on_shutdown() -> None:
	if serial_manager is not None:
		serial_manager.stop()
	task: asyncio.Task = app.state.broadcast_task
	task.cancel()
	try:
		await task
	except Exception:
		pass


@app.get("/api/health")
async def health() -> Dict[str, str]:
	return {"status": "ok", "fts": "on" if fts_enabled else "off", "device_code": app.state.device.get("device_code", "")} if isinstance(app.state.device, dict) else {"status": "ok"}


@app.get("/api/device")
async def get_device() -> Dict[str, Any]:
	return app.state.device


@app.get("/api/config")
async def get_config() -> Dict[str, Any]:
	if CONFIG_PATH.exists():
		return json.loads(CONFIG_PATH.read_text("utf-8"))
	return {
		"port": "",
		"baudrate": 115200,
		"bytesize": 8,
		"stopbits": 1,
		"parity": "N",
		"timeout": 0.1,
		"encoding": "utf-8",
	}


@app.post("/api/config")
async def set_config(req: Request):
	body = await req.json()
	required = ["port"]
	for k in required:
		if k not in body:
			return JSONResponse(status_code=400, content={"error": f"missing field: {k}"})
	CONFIG_PATH.write_text(json.dumps(body, ensure_ascii=False, indent=2), "utf-8")
	try:
		assert serial_manager is not None, "serial manager not initialized"
		serial_manager.start(
			SerialConfig(
				port=body["port"],
				baudrate=int(body.get("baudrate", 115200)),
				bytesize=int(body.get("bytesize", 8)),
				stopbits=float(body.get("stopbits", 1)),
				parity=str(body.get("parity", "N")),
				timeout=float(body.get("timeout", 0.1)),
				encoding=str(body.get("encoding", "utf-8")),
			)
		)
	except Exception as e:
		return JSONResponse(status_code=400, content={"error": str(e)})
	return {"ok": True}


@app.get("/api/logs")
async def get_logs(limit: int = 200, offset: int = 0, q: Optional[str] = None, level: Optional[str] = None,
				   time_from: Optional[str] = None, time_to: Optional[str] = None, device_code: Optional[str] = None) -> Dict[str, Any]:
	items, total = query_logs_with_total(limit=limit, offset=offset, q=q, level=level, time_from=time_from, time_to=time_to, device_code=device_code)
	return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get("/api/logs/export")
async def export_logs(format: str = "txt", device_code: Optional[str] = None):
	items, _ = query_logs_with_total(limit=10000, device_code=device_code)
	if format == "csv":
		lines = ["ts,level,content"]
		for row in reversed(items):
			content = row['content'].replace('"', '""').replace('\n', '\\n').replace('\r', '\\r')
			lines.append(f"{row['ts']},{row.get('level','')},\"{content}\"")
		text = "\n".join(lines)
		return PlainTextResponse(text, media_type="text/csv")
	else:
		text = "\n".join(row["content"] for row in reversed(items))
		return PlainTextResponse(text, media_type="text/plain")


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
	await ws.accept()
	active_clients.add(ws)
	try:
		while True:
			await ws.receive_text()
	except WebSocketDisconnect:
		pass
	except Exception:
		pass
	finally:
		try:
			active_clients.discard(ws)
			await ws.close()
		except Exception:
			pass


@app.websocket("/ws/view/{device_code}")
async def ws_view(ws: WebSocket, device_code: str):
	await ws.accept()
	room = code_to_clients.get(device_code)
	if room is None:
		room = set()
		code_to_clients[device_code] = room
	room.add(ws)
	try:
		while True:
			await ws.receive_text()
	except WebSocketDisconnect:
		pass
	except Exception:
		pass
	finally:
		try:
			room.discard(ws)
			await ws.close()
		except Exception:
			pass


if (STATIC_DIR / "index.html").exists():
	app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")


if __name__ == "__main__":
	import uvicorn
	uvicorn.run("backend.main:app", host="0.0.0.0", port=8000) 