Demonstrates Server-Sent Events with optional Brotli compression.

Run:
```bash
nim c -r app.nim
```

Test endpoints:
- http://localhost:8080/ - Index page with SSE client
- http://localhost:8080/events - Basic SSE stream
- http://localhost:8080/compressed - SSE with Brotli compression

Or test with curl:
```bash
curl -N http://localhost:8080/events
```
