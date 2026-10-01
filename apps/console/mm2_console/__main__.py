import signal
import sys
import threading

from .app import App
from .config import Config
from .server import ConsoleServer
from .telegram import TelegramBot


def main():
    try:
        config = Config.from_env()
        app = App(config)
        server = ConsoleServer((config.host, config.port), app)
    except Exception:
        print("mm2 console startup failed; check bounded configuration and private runtime files", file=sys.stderr)
        return 1
    bot = TelegramBot(app)
    stopping = threading.Event()

    def stop(*_):
        if not stopping.is_set():
            stopping.set()
            threading.Thread(target=server.shutdown, name="console-shutdown", daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    app.start()
    bot.start()
    print("mm2 console ready; private credentials are not logged", flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
        bot.close()
        app.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
