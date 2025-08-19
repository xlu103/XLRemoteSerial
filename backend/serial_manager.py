import threading
import time
from typing import Callable, Optional

try:
    import serial  # type: ignore
except Exception:  # pragma: no cover
    serial = None  # 延迟导入失败时占位，实际运行需安装 pyserial


class SerialConfig:
    def __init__(
        self,
        port: str,
        baudrate: int = 115200,
        bytesize: int = 8,
        stopbits: float = 1,
        parity: str = "N",
        timeout: float = 0.1,
        encoding: str = "utf-8",
    ) -> None:
        self.port = port
        self.baudrate = baudrate
        self.bytesize = bytesize
        self.stopbits = stopbits
        self.parity = parity
        self.timeout = timeout
        self.encoding = encoding


class SerialManager:
    def __init__(self, on_line: Callable[[str], None]) -> None:
        self._on_line = on_line
        self._config: Optional[SerialConfig] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._serial: Optional["serial.Serial"] = None
        self._lock = threading.Lock()

    def start(self, config: SerialConfig) -> None:
        with self._lock:
            self.stop()
            self._config = config
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run, name="SerialReader", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)
        self._thread = None
        if self._serial is not None:
            try:
                self._serial.close()
            except Exception:
                pass
            self._serial = None

    def _open_serial(self, cfg: SerialConfig) -> None:
        if serial is None:
            raise RuntimeError("pyserial 未安装。请先安装 pyserial 包")
        self._serial = serial.Serial(
            port=cfg.port,
            baudrate=cfg.baudrate,
            bytesize=cfg.bytesize,
            stopbits=cfg.stopbits,
            parity=cfg.parity,
            timeout=cfg.timeout,
        )

    def _run(self) -> None:
        backoff_s = 0.5
        while not self._stop_event.is_set():
            cfg = self._config
            if cfg is None:
                time.sleep(0.2)
                continue
            try:
                if self._serial is None or not self._serial.is_open:
                    self._open_serial(cfg)
                    backoff_s = 0.5
                line: bytes = self._serial.readline() if self._serial else b""
                if not line:
                    continue
                try:
                    text = line.decode(cfg.encoding, errors="replace").rstrip("\r\n")
                except Exception:
                    text = line.decode("utf-8", errors="replace").rstrip("\r\n")
                self._on_line(text)
            except Exception:
                # 出错后退避重试
                try:
                    if self._serial is not None:
                        self._serial.close()
                except Exception:
                    pass
                self._serial = None
                time.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, 5.0)

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def current_config(self) -> Optional[SerialConfig]:
        return self._config 