/**
 * Two benchmarks, because they answer different questions.
 *
 *   1. Core   — how long does one limiter decision take, with HTTP out of the
 *               picture? This is the number that decides whether the limiter
 *               is affordable.
 *   2. HTTP   — what does the service sustain end to end, and what does the
 *               limiter add on top of the same route without it?
 *
 * Run:  npm run bench            (core only, no server needed)
 *       npm run bench -- --http  (also drives a running server with autocannon)
 */
export {};
