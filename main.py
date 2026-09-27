"""本地启动入口：uv run python main.py 后访问 http://127.0.0.1:8000/docs"""
import logging

import uvicorn

from app.api import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)

app = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")
