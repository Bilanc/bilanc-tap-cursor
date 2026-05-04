# bilanc-tap-cursor

A [Singer](https://www.singer.io/) tap for extracting team usage and analytics data from Cursor.

## Streams

This tap currently exposes:

- `daily_usage`
- `usage_events`
- `ai_commit_metrics`

## Configuration

Create a `config.json` file:

```json
{
  "api_key": "your_cursor_api_key",
  "start_date": "2026-01-01T00:00:00Z",
  "request_timeout": 300
}
```

- `api_key` (required unless `CURSOR_API_KEY` env var is set): Cursor API key.
- `start_date` (required): Start date for incremental sync.
- `request_timeout` (optional): Request timeout in seconds (defaults to `300`).

## Usage

```bash
# Install locally
pip install -e .

# Discovery mode
tap-cursor --config config.json --discover > catalog.json

# Sync mode
tap-cursor --config config.json --catalog catalog.json
```
