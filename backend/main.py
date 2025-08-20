import asyncio
import json
import os
import sqlite3
import secrets
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, DefaultDict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, PlainTextResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# 移除串口管理器导入，改为纯Web模式
# from backend.serial_manager import SerialManager, SerialConfig

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

active_clients: Set[WebSocket] = set()
code_to_clients: Dict[str, Set[WebSocket]] = {}
line_queue: asyncio.Queue[Tuple[str, str]] = asyncio.Queue(maxsize=10000)
# 移除串口管理器实例
# serial_manager: Optional[SerialManager] = None
_db_lock = asyncio.Lock()
fts_enabled: bool = False
app.state.device: Dict[str, Any] = {}
_rate_limit_cache: Dict[str, List[float]] = {}


def _enqueue_line_for_device(device_code: str, text: str) -> None:
	"""接收来自Web端的串口数据并广播给所有查看者"""
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
				content TEXT NOT NULL,
				device_id INTEGER
			);
			"""
		)
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS device (
				id INTEGER PRIMARY KEY,
				device_code TEXT UNIQUE NOT NULL,
				created_at TEXT NOT NULL,
				control_secret TEXT
			);
			"""
		)
		conn.execute("CREATE INDEX IF NOT EXISTS idx_log_ts ON log(ts);")
		conn.execute("CREATE INDEX IF NOT EXISTS idx_log_device_ts ON log(device_id, ts);")
		conn.commit()
	migrate_add_columns()
	init_fts()


def migrate_add_columns() -> None:
	with db_conn() as conn:
		cols = [r[1] for r in conn.execute("PRAGMA table_info(device)").fetchall()]
		if "control_secret" not in cols:
			conn.execute("ALTER TABLE device ADD COLUMN control_secret TEXT;")
			conn.commit()


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
	"""确保设备存在，如果不存在则创建新设备"""
	with db_conn() as conn:
		row = conn.execute("SELECT id, device_code, created_at, control_secret FROM device LIMIT 1").fetchone()
		if row:
			return {"id": row[0], "device_code": row[1], "created_at": row[2], "has_control_secret": bool(row[3])}
		code = _generate_device_code(8)
		created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
		cur = conn.execute("INSERT INTO device (device_code, created_at) VALUES (?, ?)", (code, created_at))
		conn.commit()
		dev_id = cur.lastrowid
		return {"id": dev_id, "device_code": code, "created_at": created_at, "has_control_secret": False}

def create_device_for_connection(port_name: str) -> Dict[str, Any]:
	"""为新的串口连接创建设备"""
	code = _generate_device_code(8)
	created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
	
	with db_conn() as conn:
		cur = conn.execute("INSERT INTO device (device_code, created_at) VALUES (?, ?)", (code, created_at))
		conn.commit()
		dev_id = cur.lastrowid
		
		# 记录连接信息
		conn.execute("INSERT INTO log (ts, level, content, device_id) VALUES (?, ?, ?, ?)", 
					(created_at, "info", f"新设备连接: {port_name}", dev_id))
		conn.commit()
	
	return {"id": dev_id, "device_code": code, "created_at": created_at, "has_control_secret": False}


def insert_log(content: str, level: str = "info", device_id: Optional[int] = None) -> None:
	ts = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
	with db_conn() as conn:
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
	"""应用启动时初始化数据库和设备"""
	init_db()
	app.state.device = ensure_device()
	app.state.broadcast_task = asyncio.create_task(broadcaster())


@app.on_event("shutdown")
async def on_shutdown() -> None:
	"""应用关闭时清理资源"""
	task: asyncio.Task = app.state.broadcast_task
	task.cancel()
	try:
		await task
	except Exception:
		pass


@app.get("/api/health")
async def health() -> Dict[str, Any]:
	return {
		"status": "ok",
		"mode": "web_serial",  # 标记为Web串口模式
		"fts": "on" if fts_enabled else "off",
		"device_code": app.state.device.get("device_code", ""),
		"has_control_secret": app.state.device.get("has_control_secret", False),
		"active_clients": len(active_clients),
		"total_viewers": sum(len(clients) for clients in code_to_clients.values())
	}


@app.get("/api/device")
async def get_device() -> Dict[str, Any]:
	return app.state.device


def _is_local_request(req: Request) -> bool:
	try:
		host = (req.client.host or "").strip()
		return host in {"127.0.0.1", "::1", "localhost"}
	except Exception:
		return False


@app.post("/api/device/secret")
async def set_device_secret(req: Request):
	if not _is_local_request(req):
		return JSONResponse(status_code=403, content={"error": "forbidden"})
	body = await req.json()
	secret = str(body.get("secret", "")).strip()
	if not secret or len(secret) < 6:
		return JSONResponse(status_code=400, content={"error": "secret too short"})
	with db_conn() as conn:
		conn.execute("UPDATE device SET control_secret = ? WHERE id = ?", (secret, int(app.state.device["id"])) )
		conn.commit()
	app.state.device["has_control_secret"] = True
	return {"ok": True}


@app.post("/api/control/send")
async def control_send(req: Request):
	"""远程控制命令发送API - 现在通过Web端转发"""
	body = await req.json()
	code = str(body.get("device_code", "")).strip()
	token = str(body.get("token", "")).strip()
	data = str(body.get("data", ""))
	append_newline = bool(body.get("append_newline", True))
	if not code or not token or not data:
		return JSONResponse(status_code=400, content={"error": "missing fields"})
	if code != app.state.device.get("device_code"):
		return JSONResponse(status_code=404, content={"error": "device not found"})
	with db_conn() as conn:
		row = conn.execute("SELECT control_secret FROM device WHERE device_code = ?", (code,)).fetchone()
		if not row or not row[0] or row[0] != token:
			return JSONResponse(status_code=403, content={"error": "invalid token"})
	
	# 记录控制命令到日志
	ip = (req.client.host or "unknown")
	now = time.time()
	lst = _rate_limit_cache.setdefault(ip, [])
	lst[:] = [t for t in lst if now - t < 2.0]
	if len(lst) >= 10:
		return JSONResponse(status_code=429, content={"error": "rate limited"})
	lst.append(now)
	
	# 通过WebSocket广播控制命令到Web端
	control_message = f"[CONTROL] {data}"
	_enqueue_line_for_device(code, control_message)
	
	return {"ok": True, "message": "控制命令已发送到Web端"}

@app.post("/api/serial/data")
async def receive_serial_data(req: Request):
	"""接收来自Web端的串口数据"""
	body = await req.json()
	device_code = str(body.get("device_code", "")).strip()
	data = str(body.get("data", "")).strip()
	
	if not device_code or not data:
		return JSONResponse(status_code=400, content={"error": "missing device_code or data"})
	
	# 将数据放入队列，广播给所有查看者
	_enqueue_line_for_device(device_code, data)
	
	return {"ok": True, "message": "数据已接收并广播"}

@app.post("/api/device/create")
async def create_device(req: Request):
	"""为新的串口连接创建设备"""
	body = await req.json()
	port_name = str(body.get("port_name", "")).strip()
	
	if not port_name:
		return JSONResponse(status_code=400, content={"error": "missing port_name"})
	
	try:
		device = create_device_for_connection(port_name)
		return {"ok": True, "device": device}
	except Exception as e:
		return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/device/list")
async def list_devices():
	"""获取所有设备列表"""
	with db_conn() as conn:
		rows = conn.execute("SELECT id, device_code, created_at, control_secret FROM device ORDER BY created_at DESC").fetchall()
		devices = []
		for row in rows:
			devices.append({
				"id": row[0],
				"device_code": row[1],
				"created_at": row[2],
				"has_control_secret": bool(row[3])
			})
		return {"ok": True, "devices": devices}


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


# 页面入口：/ -> portal，/control -> 本机控制，/viewer -> 远程查看
@app.get("/")
async def page_portal() -> FileResponse:
	return FileResponse(str(STATIC_DIR / "portal.html"))


@app.get("/control")
async def page_control() -> FileResponse:
	return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/viewer")
async def page_viewer() -> FileResponse:
	return FileResponse(str(STATIC_DIR / "viewer.html"))


# 静态资源目录
app.mount("/static", StaticFiles(directory=str(STATIC_DIR), html=False), name="static")


if __name__ == "__main__":
	import uvicorn
	uvicorn.run("backend.main:app", host="0.0.0.0", port=8000) 