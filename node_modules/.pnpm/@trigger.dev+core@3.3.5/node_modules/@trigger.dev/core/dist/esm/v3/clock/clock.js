export function calculateDurationInMs(start, end) {
    const [startSeconds, startNanoseconds] = start;
    const [endSeconds, endNanoseconds] = end;
    const seconds = endSeconds - startSeconds;
    const nanoseconds = endNanoseconds - startNanoseconds;
    return Math.floor(seconds * 1000 + nanoseconds / 1000000);
}
//# sourceMappingURL=clock.js.map