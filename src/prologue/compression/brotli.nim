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

## Streaming Brotli compression for Prologue.
##
## This module provides streaming compression for handlers that need to send
## data incrementally (like SSE, chunked responses, etc).
##
## Example:
## ```nim
## import prologue
## import prologue/compression/brotli
##
## proc streamHandler(ctx: Context) {.async.} =
##   let stream = await initBrotliStream(ctx)
##   defer: stream.close()
##
##   await stream.send("chunk 1")
##   await stream.send("chunk 2")
##   await stream.finish()  # finalize compression
## ```

when not (compiles do: import brotli/encode):
  {.error: "Brotli compression requires the 'brotli' package. Install with: nimble install brotli".}

import brotli/encode
import ../core/context
import std/asyncdispatch

type
  BrotliStream* = object
    ## A streaming Brotli compressor wrapping a Context.
    ctx: Context
    encoder: ptr BrotliEncoderState
    headersSent: bool

proc initBrotliStream*(ctx: Context, quality: int = 4, lgwin: int = 18): Future[BrotliStream] {.async.} =
  ## Creates a new streaming Brotli compressor.
  ##
  ## Parameters:
  ## - `ctx`: The Prologue context to send compressed data to.
  ## - `quality`: Compression quality (0-11). Lower = faster, higher = smaller.
  ##   Default 4 is good for real-time streaming.
  ## - `lgwin`: Window size as log2 (10-24). Default 18 = 256KB window.
  ##
  ## Note: This does NOT send HTTP headers automatically. The caller is
  ## responsible for sending appropriate headers with `Content-Encoding: br`.
  let encoder = BrotliEncoderCreateInstance(nil, nil, nil)
  if encoder.isNil:
    raise newException(IOError, "Failed to create Brotli encoder")

  discard BrotliEncoderSetParameter(encoder, BROTLI_PARAM_QUALITY, uint32(quality))
  discard BrotliEncoderSetParameter(encoder, BROTLI_PARAM_LGWIN, uint32(lgwin))

  result = BrotliStream(ctx: ctx, encoder: encoder, headersSent: false)

proc close*(stream: BrotliStream) =
  ## Closes the Brotli encoder and frees resources.
  ## Always call this when done with the stream.
  if stream.encoder != nil:
    BrotliEncoderDestroyInstance(stream.encoder)

proc compressAndFlush(stream: BrotliStream, data: string, op: BrotliEncoderOperation): string =
  ## Internal: compress data with specified operation.
  if data.len == 0 and op == BROTLI_OPERATION_PROCESS:
    return ""

  var
    inputData = data
    availableIn = csize_t(data.len)
    nextIn = if data.len > 0: cast[ptr uint8](addr inputData[0]) else: nil
    outputBuf = newString(max(data.len * 2 + 64, 1024))
    availableOut = csize_t(outputBuf.len)
    nextOut = cast[ptr uint8](addr outputBuf[0])
    totalOut: csize_t

  let success = BrotliEncoderCompressStream(
    stream.encoder,
    op,
    addr availableIn,
    cast[ptr ptr uint8](addr nextIn),
    addr availableOut,
    cast[ptr ptr uint8](addr nextOut),
    addr totalOut
  )

  if success != 1:
    return ""

  let bytesWritten = outputBuf.len - int(availableOut)
  if bytesWritten > 0:
    result = outputBuf[0 ..< bytesWritten]
  else:
    result = ""

proc send*(stream: BrotliStream, data: string) {.async.} =
  ## Sends data through the compressor with flush.
  ## Data is compressed and flushed immediately so the client receives it.
  let compressed = compressAndFlush(stream, data, BROTLI_OPERATION_FLUSH)
  if compressed.len > 0:
    await stream.ctx.send(compressed)

proc sendRaw*(stream: BrotliStream, data: string) {.async.} =
  ## Sends data without compression (pass-through).
  ## Useful for sending pre-compressed or binary data.
  await stream.ctx.send(data)

proc finish*(stream: BrotliStream) {.async.} =
  ## Finalizes the Brotli stream.
  ## Call this when done sending data to properly close the compression stream.
  let compressed = compressAndFlush(stream, "", BROTLI_OPERATION_FINISH)
  if compressed.len > 0:
    await stream.ctx.send(compressed)
