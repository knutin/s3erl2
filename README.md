# S3

This is a rewrite of the much forked s3erl library, originally written
by Andrew Birkett <andy@nobugs.org>.

The purpose of the reworking is to make the library more robust, so
you can be confident it will (mostly) behave correctly.

Focus:
 * Tests
 * Simplicity, handle only get, put and delete
 * Error handling, retries
 * Sensible timeouts
 * Callbacks for custom logging of events


## Timeouts

Caller timeout: 5000

Attempts: 3 (allows 2 retries)

http timeout: x

5000 = 3 * x
5000 / 3 = x


## Running the tests

To run the unit tests, you need to use your own S3 credentials. These
should be placed in priv/s3_credentials.term according to the template
in priv/s3_credentials.term.template.