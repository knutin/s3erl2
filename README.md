# S3

This is a work of the much forked s3erl library, originally written by Andrew Birkett <andy@nobugs.org>.

The purpose of the reworking is to make the library more robust, so you can be confident it will (mostly) behave correctly.

Focus:
 * Simplicity, handle only get and put
 * Error handling, retries
 * Sensible timeouts
 * Callbacks for custom logging of events


## Timeouts

Caller timeout: 5000

Attempts: 3 (allows 2 retries)

http timeout: x

5000 = 3 * x
5000 / 3 = x
