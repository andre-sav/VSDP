#!/bin/zsh
cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
pip -q install -r requirements.txt
streamlit run app.py
