import prologue
import prologue/sse
import std/[asyncdispatch, options, times]


proc eventsHandler*(ctx: Context) {.async.} =
  ## Basic SSE handler - no compression
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


proc compressedHandler*(ctx: Context) {.async.} =
  ## SSE with Brotli compression
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


proc indexHandler*(ctx: Context) {.async.} =
  ## Index page with SSE client
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
