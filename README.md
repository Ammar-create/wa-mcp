# wa-cli

Headless WhatsApp client built on [`whatsapp-rust`](https://crates.io/crates/whatsapp-rust).
No browser, no GUI, no X server. Links with an 8-character pair code.

    wa pair 923001234567        # prints PAIR_CODE=XXXXXXXX
    wa serve --port 8899        # headless bot + local JSON API

API: `GET /status`, `GET /chats`, `GET /messages?chat=&limit=`, `POST /send {"to","text"}`.

The MCP server in `mcp/` wraps that API.
