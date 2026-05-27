# GenNet — Federated Genomic Intelligence

A federated network for rare genetic disease (SLC6A1 / DEE-49) — proof of concept.

**Live:** _(deployed on Render)_

## What this is

Phase 1 of GenNet's collaborative mode: a join-and-approve layer.

- Doctors request to join the network (hospital name + doctor name).
- The admin (Rafael) reviews requests from a private panel.
- Approved hospitals appear on the public global map.
- Each approved hospital gets a personal code to identify their node.

## What this is NOT (yet)

These come in the next iterations:
- Viewer integration (per-hospital patient entry)
- Variant aggregation across the network
- Ensembl enrichment layer

## Tech stack

- Python 3.11 + FastAPI
- Jinja2 templates
- JSON file storage (will move to Postgres in production)
- Deployed on Render

## Running locally

```bash
pip install -r requirements.txt
ADMIN_PASSWORD=your-password python app.py
```

Open http://localhost:8000

## Author

Rafael Campoy Ramírez — rcr216@inlumine.ual.es
