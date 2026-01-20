import prologue
import ./views


const urlPatterns* = @[
  pattern("/", indexHandler),
  pattern("/events", eventsHandler),
  pattern("/compressed", compressedHandler)
]
