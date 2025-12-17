# VanillaSoft Streamlit App (Superior UI + Hardcoded Template Schema)

This package keeps the "wizard-style" UI from your latest `app.py` while implementing the updated schema logic:

- Output columns match the selected data source template **exactly** (order preserved)
- The **only** additional output column is `Import Notes`
  - Filled with a Zoho CRM URL only when a Locatings match is found (otherwise blank)

## Run locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Zoho (optional)
Copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml` and fill in your credentials.
