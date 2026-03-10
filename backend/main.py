from __future__ import annotations

import uvicorn

from backend.workspace_api import create_workspace_app


app = create_workspace_app()


def run() -> None:
    uvicorn.run(app, host="127.0.0.1", port=8765, log_level="warning")


if __name__ == "__main__":
    run()
