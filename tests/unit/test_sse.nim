# Unit tests for SSE module
#
# Run: nim c -r tests/unit/test_sse.nim

import std/[unittest, options]
import ../../src/prologue/sse/sse

suite "BrotliConfig":
  test "brotli() creates default config":
    let cfg = brotli()
    check cfg.quality == 4
    check cfg.lgwin == 18

  test "brotli() accepts custom values":
    let cfg = brotli(quality = 6, lgwin = 20)
    check cfg.quality == 6
    check cfg.lgwin == 20

suite "hasBrotli constant":
  test "hasBrotli is true when brotli package installed":
    # This test verifies the compile-time detection works
    check hasBrotli == true

when isMainModule:
  discard
