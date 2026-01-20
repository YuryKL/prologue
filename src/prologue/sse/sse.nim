# Copyright 2026 Prologue Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## Server-Sent Events (SSE) support for Prologue.
##
## SSE provides a simple way to push  updates from server to client
## over HTTP. Unlike WebSockets, SSE is unidirectional (server to client only)
## but simpler to use and works over standard HTTP keep-alive connection. It also
## enables you to utilize HTTP niceties that Websockets don't provide out of the box,
## such as auto-reconnect, compression, ordered delivery, etc
##
####
## Example:
## ```nim
##
## proc eventsHandler(ctx: Context) {.async.} =
##   let sse = await initSSE(ctx)
##   var counter = 0
##   while true:
##     inc counter
##     await sse.sendEvent("Counter: " & $counter, event = "tick")
##     await sleepAsync(1000)
##
## let app = newApp()
## app.get("/events", eventsHandler)
## app.run()
## ```
##
## With Brotli compression:
## ```nim
##
## proc eventsHandler(ctx: Context) {.async.} =
##   let sse = await initSSE(ctx, compression = some(brotli()))
##   defer: sse.close()
##   # ... send events ...
## ```

import ../core/context

import std/[asyncdispatch, options, strutils]

# Optional brotli compression support
when (compiles do: import ../compression/brotli):
  import ../compression/brotli
  const hasBrotli* = true
else:
  const hasBrotli* = false

type
  BrotliConfig* = object
    ## Configuration for Brotli compression.
    quality*: int  ## 0-11, lower = faster, higher = smaller. Default 4.
    lgwin*: int    ## Log2 window size (10-24). Default 18 = 256KB.

  SSEConnection* = object
    ## Represents an active SSE connection to a client.
    ctx: Context
    compressed: bool
    when hasBrotli:
      brotliStream: BrotliStream

const
  SseHeaders = "HTTP/1.1 200 OK\r\n" &
               "Content-Type: text/event-stream\r\n" &
               "Cache-Control: no-cache\r\n" &
               "Connection: keep-alive\r\n" &
               "\r\n"
  SseHeadersBrotli = "HTTP/1.1 200 OK\r\n" &
                     "Content-Type: text/event-stream\r\n" &
                     "Cache-Control: no-cache\r\n" &
                     "Connection: keep-alive\r\n" &
                     "Content-Encoding: br\r\n" &
                     "\r\n"

proc brotli*(quality = 4, lgwin = 18): BrotliConfig =
  ## Creates Brotli compression config with optional tuning.
  BrotliConfig(quality: quality, lgwin: lgwin)

proc initSSE*(ctx: Context, compression = none(BrotliConfig)): Future[SSEConnection] {.async.} =
  ## Initializes an SSE connection by sending the appropriate HTTP headers.
  ##
  ## Parameters:
  ## - `ctx`: The Prologue context for the connection.
  ## - `compression`: Optional Brotli compression config. Use `brotli()` to enable.
  ##
  ## This sends:
  ## - `Content-Type: text/event-stream`
  ## - `Cache-Control: no-cache`
  ## - `Connection: keep-alive`
  ## - `Content-Encoding: br` (when compression is enabled)
  ##
  ## Example:
  ## ```nim
  ## # Without compression
  ## let sse = await initSSE(ctx)
  ##
  ## # With Brotli compression (default settings)
  ## let sse = await initSSE(ctx, compression = some(brotli()))
  ##
  ## # With custom Brotli settings
  ## let sse = await initSSE(ctx, compression = some(brotli(quality = 6, lgwin = 20)))
  ## ```
  ##
  ## Returns an `SSEConnection` that can be used to send events.
  if compression.isSome:
    when hasBrotli:
      let cfg = compression.get()
      await ctx.send(SseHeadersBrotli)
      ctx.handled = true
      let stream = await initBrotliStream(ctx, quality = cfg.quality, lgwin = cfg.lgwin)
      result = SSEConnection(ctx: ctx, compressed: true, brotliStream: stream)
    else:
      raise newException(ValueError,
        "Brotli compression requested but 'brotli' package is not installed. " &
        "Install with: nimble install brotli")
  else:
    await ctx.send(SseHeaders)
    ctx.handled = true
    when hasBrotli:
      result = SSEConnection(ctx: ctx, compressed: false)
    else:
      result = SSEConnection(ctx: ctx, compressed: false)

proc sendEvent*(sse: SSEConnection, data: string, event = "",
                id = "", retry = -1) {.async.} =
  ## Sends an SSE event to the client.
  ##
  ## Parameters:
  ## - `data`: The event data (required). Multi-line data is supported.
  ## - `event`: Optional event type name. Clients can listen for specific
  ##   event types using `addEventListener`.
  ## - `id`: Optional event ID. Clients use this for reconnection via
  ##   `Last-Event-ID` header.
  ## - `retry`: Optional reconnection time in milliseconds. Tells the client
  ##   how long to wait before reconnecting if the connection is lost.
  ##
  ## Example:
  ## ```nim
  ## # Simple data-only event
  ## await sse.sendEvent("Hello, world!")
  ##
  ## # Named event with ID
  ## await sse.sendEvent("User logged in", event = "login", id = "12345")
  ##
  ## # Multi-line data
  ## await sse.sendEvent("Line 1\nLine 2\nLine 3")
  ## ```
  var msg = ""
  if id.len > 0:
    msg.add "id: " & id & "\n"
  if event.len > 0:
    msg.add "event: " & event & "\n"
  if retry >= 0:
    msg.add "retry: " & $retry & "\n"
  for line in data.splitLines:
    msg.add "data: " & line & "\n"
  msg.add "\n"
  if sse.compressed:
    when hasBrotli:
      await sse.brotliStream.send(msg)
    else:
      await sse.ctx.send(msg)
  else:
    await sse.ctx.send(msg)

proc sendComment*(sse: SSEConnection, comment: string) {.async.} =
  ## Sends an SSE comment to the client.
  ##
  ## Comments are prefixed with `:` and are ignored by the client's
  ## EventSource API. They are useful for:
  ## - Keep-alive pings to prevent connection timeout
  ## - Debugging information
  ##
  ## Example:
  ## ```nim
  ## await sse.sendComment("keep-alive")
  ## ```
  let msg = ": " & comment & "\n\n"
  if sse.compressed:
    when hasBrotli:
      await sse.brotliStream.send(msg)
    else:
      await sse.ctx.send(msg)
  else:
    await sse.ctx.send(msg)

proc sendRetry*(sse: SSEConnection, milliseconds: int) {.async.} =
  ## Sends a retry directive to the client.
  ##
  ## This tells the client how long to wait (in milliseconds) before
  ## attempting to reconnect if the connection is lost.
  ##
  ## Example:
  ## ```nim
  ## # Tell client to wait 5 seconds before reconnecting
  ## await sse.sendRetry(5000)
  ## ```
  let msg = "retry: " & $milliseconds & "\n\n"
  if sse.compressed:
    when hasBrotli:
      await sse.brotliStream.send(msg)
    else:
      await sse.ctx.send(msg)
  else:
    await sse.ctx.send(msg)

proc close*(sse: SSEConnection) =
  ## Closes the SSE connection and cleans up resources.
  ##
  ## When using compression, this finalizes and closes the Brotli stream.
  ## Always call this when done with a compressed SSE connection.
  ##
  ## Example:
  ## ```nim
  ## let sse = await initSSE(ctx, compression = some(brotli()))
  ## defer: sse.close()
  ## # ... send events ...
  ## ```
  if sse.compressed:
    when hasBrotli:
      sse.brotliStream.close()
