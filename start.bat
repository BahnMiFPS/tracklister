@echo off
cd /d "%~dp0"
if not exist .venv (
  python -m venv .venv
  .venv\Scripts\python.exe -m pip install -r requirements.txt
)
start "" http://127.0.0.1:8765
.venv\Scripts\python.exe -m uvicorn app:app --host 127.0.0.1 --port 8765
