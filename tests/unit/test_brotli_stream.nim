# Unit tests for streaming Brotli compression
#
# Run: nim c -r tests/unit/test_brotli_stream.nim

import std/[unittest, strutils]
import brotli/encode

suite "Brotli Encoder":
  test "encoder can be created":
    let encoder = BrotliEncoderCreateInstance(nil, nil, nil)
    check encoder != nil
    BrotliEncoderDestroyInstance(encoder)

  test "encoder accepts quality parameter":
    let encoder = BrotliEncoderCreateInstance(nil, nil, nil)
    check encoder != nil
    let result = BrotliEncoderSetParameter(encoder, BROTLI_PARAM_QUALITY, 4)
    check result == 1
    BrotliEncoderDestroyInstance(encoder)

  test "encoder accepts lgwin parameter":
    let encoder = BrotliEncoderCreateInstance(nil, nil, nil)
    check encoder != nil
    let result = BrotliEncoderSetParameter(encoder, BROTLI_PARAM_LGWIN, 18)
    check result == 1
    BrotliEncoderDestroyInstance(encoder)

  test "can compress simple string":
    let encoder = BrotliEncoderCreateInstance(nil, nil, nil)
    check encoder != nil
    defer: BrotliEncoderDestroyInstance(encoder)

    discard BrotliEncoderSetParameter(encoder, BROTLI_PARAM_QUALITY, 4)

    let original = "Hello, World! ".repeat(100)
    var
      inputData = original
      availableIn = csize_t(original.len)
      nextIn = cast[ptr uint8](addr inputData[0])
      outputBuf = newString(original.len + 1024)
      availableOut = csize_t(outputBuf.len)
      nextOut = cast[ptr uint8](addr outputBuf[0])
      totalOut: csize_t

    let success = BrotliEncoderCompressStream(
      encoder,
      BROTLI_OPERATION_FINISH,
      addr availableIn,
      cast[ptr ptr uint8](addr nextIn),
      addr availableOut,
      cast[ptr ptr uint8](addr nextOut),
      addr totalOut
    )

    check success == 1
    let compressedLen = outputBuf.len - int(availableOut)
    check compressedLen > 0
    check compressedLen < original.len  # Should compress

when isMainModule:
  discard
