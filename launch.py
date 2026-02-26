#!/usr/bin/env python3
"""
launch.py  —  OLAP Intelligence Platform
-----------------------------------------
Just run:  python launch.py

This script does everything automatically:
  1. Checks Python version
  2. Creates a virtual environment (.venv)
  3. Installs all dependencies
  4. Loads your API key from .env (or asks once)
  5. Generates the dataset if needed
  6. Starts the server
  7. Opens http://localhost:8000 in your browser
"""

import sys
import os
import subprocess
import time
import webbrowser
import threading
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).parent.resolve()
VENV       = ROOT / ".venv"
REQS       = ROOT / "requirements.txt"
CSV_PATH   = ROOT / "data" / "global_retail_sales.csv"
GEN_SCRIPT = ROOT / "data" / "generate_dataset.py"
ENV_FILE   = ROOT / ".env"
PORT       = 8000
HOST       = "127.0.0.1"

# ── Terminal colours ──────────────────────────────────────────────────────────
def _c(code, text): return f"\033[{code}m{text}\033[0m"
def ok(msg):   print(_c("92", f"  ✓  {msg}"))
def info(msg): print(_c("96", f"  →  {msg}"))
def warn(msg): print(_c("93", f"  ⚠  {msg}"))
def fail(msg): print(_c("91", f"  ✗  {msg}")); sys.exit(1)

def banner():
    print(_c("1;96", """
  ╔═══════════════════════════════════════════════════╗
  ║       OLAP Intelligence Platform  —  Launcher     ║
  ║                   Multi-Agent BI                  ║
  ╚═══════════════════════════════════════════════════╝
"""))

# ── Helpers ───────────────────────────────────────────────────────────────────
def venv_exe(name):
    """Return path to an executable inside the venv."""
    if sys.platform == "win32":
        return VENV / "Scripts" / f"{name}.exe"
    return VENV / "bin" / name

def run(cmd, **kwargs):
    return subprocess.run(cmd, **kwargs)

# ── Steps ─────────────────────────────────────────────────────────────────────

def check_python():
    info("Checking Python version...")
    v = sys.version_info
    if v < (3, 9):
        fail(f"Python 3.9+ required — you have {v.major}.{v.minor}. Get it at https://python.org")
    ok(f"Python {v.major}.{v.minor}.{v.micro}")


def setup_venv():
    py = venv_exe("python")
    if py.exists():
        ok("Virtual environment ready")
        return
    info("Creating virtual environment (.venv)  [first run only]...")
    r = run([sys.executable, "-m", "venv", str(VENV)], capture_output=True, text=True)
    if r.returncode != 0:
        fail(f"Could not create venv:\n{r.stderr}")
    ok("Virtual environment created")


def install_deps():
    pip = venv_exe("pip")
    info("Installing dependencies  [~30 seconds on first run]...")
    r = run([str(pip), "install", "-r", str(REQS), "-q"],
            capture_output=True, text=True)
    if r.returncode != 0:
        # Show the last part of the error so the user knows what failed
        print("\n" + r.stderr[-2000:])
        fail("pip install failed — see error above")
    ok("Dependencies installed")


def load_env():
    """Load .env file into os.environ."""
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def check_api_key():
    # Already in environment?
    if os.environ.get("ANTHROPIC_API_KEY", "").startswith("sk-ant"):
        ok("Anthropic API key loaded")
        return

    warn("No Anthropic API key found.")
    print("  The AI chat uses Claude — get a free key at https://console.anthropic.com")
    print("  Without it the system still works using the built-in rule-based fallback.\n")

    try:
        key = input(_c("96", "  Paste your API key (or press Enter to skip): ")).strip()
    except (EOFError, KeyboardInterrupt):
        key = ""

    if key.startswith("sk-ant"):
        ENV_FILE.write_text(f"ANTHROPIC_API_KEY={key}\n", encoding="utf-8")
        os.environ["ANTHROPIC_API_KEY"] = key
        ok("API key saved to .env")
    else:
        warn("Skipped — running with rule-based fallback")


def ensure_dataset():
    if CSV_PATH.exists():
        ok(f"Dataset found")
        return
    info("Generating dataset (10,000 records)...")
    if not GEN_SCRIPT.exists():
        fail(f"Generator script not found: {GEN_SCRIPT}")
    py = venv_exe("python")
    r = run([str(py), str(GEN_SCRIPT)], capture_output=True, text=True, cwd=str(ROOT))
    if r.returncode != 0:
        print(r.stderr)
        fail("Dataset generation failed")
    ok("Dataset generated")


def start_server():
    py  = venv_exe("python")
    url = f"http://{HOST}:{PORT}"

    print(_c("1", f"""
  Starting server...
  ┌─────────────────────────────────────────┐
  │  App:     {url}              │
  │  Swagger: {url}/docs         │
  └─────────────────────────────────────────┘
  Press Ctrl+C to stop
"""))

    def open_later():
        time.sleep(2.5)
        try:
            webbrowser.open(url)
        except Exception:
            pass

    threading.Thread(target=open_later, daemon=True).start()

    run(
        [str(py), "-m", "uvicorn", "backend.api.main:app",
         "--host", HOST, "--port", str(PORT), "--reload"],
        cwd=str(ROOT),
        env=os.environ.copy(),
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    banner()
    check_python()
    setup_venv()
    install_deps()
    load_env()
    check_api_key()
    ensure_dataset()
    print(_c("1;92", "\n  ✓  Setup complete — launching!\n"))
    start_server()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(_c("93", "\n  Server stopped. Goodbye!\n"))
