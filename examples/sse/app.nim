# SSE Example for Prologue
#
# Demonstrates Server-Sent Events with optional Brotli compression.
#
# Run: nim c -r examples/sse/app.nim
# Test: curl -N http://localhost:8080/events

import ../../src/prologue
import ../../src/prologue/sse
import std/[asyncdispatch, options, times]

# Basic SSE handler - no compression
proc eventsHandler(ctx: Context) {.async.} =
  let sse = await initSSE(ctx)
  var counter = 0

  await sse.sendRetry(5000)  # 5s reconnect

  while true:
    inc counter
    let timestamp = now().format("HH:mm:ss")
    await sse.sendEvent(
      data = "Counter: " & $counter & " at " & timestamp,
      event = "tick",
      id = $counter
    )

    if counter mod 5 == 0:
      await sse.sendComment("keep-alive")

    await sleepAsync(1000)

# SSE with Brotli compression
proc compressedHandler(ctx: Context) {.async.} =
  let sse = await initSSE(ctx, compression = some(brotli(quality = 4)))
  defer: sse.close()

  var counter = 0
  await sse.sendRetry(5000)

  while true:
    inc counter
    await sse.sendEvent(
      data = "Compressed event #" & $counter,
      event = "update"
    )
    await sleepAsync(1000)

# Index page
proc indexHandler(ctx: Context) {.async.} =
  resp htmlResponse("""
<!DOCTYPE html>
<html>
<head><title>SSE Example</title></head>
<body>
  <h1>SSE Example</h1>
  <div id="events"></div>
  <script>
    const es = new EventSource('/events');
    es.addEventListener('tick', (e) => {
      const div = document.createElement('div');
      div.textContent = e.data;
      document.getElementById('events').appendChild(div);
    });
  </script>
</body>
</html>
""")

proc main() =
  var app = newApp()
  app.get("/", indexHandler)
  app.get("/events", eventsHandler)
  app.get("/compressed", compressedHandler)

  echo "Server: http://localhost:8080"
  echo "  /events     - Basic SSE"
  echo "  /compressed - SSE with Brotli"
  app.run()

when isMainModule:
  main()
