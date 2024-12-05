import { PreciseDate } from "@google-cloud/precise-date";
export class PreciseWallClock {
    _origin;
    get #originClockTime() {
        return this._origin.clockTime;
    }
    get #originPreciseDate() {
        return this._origin.preciseDate;
    }
    constructor(options = {}) {
        this._origin = {
            clockTime: options.origin ?? process.hrtime(),
            preciseDate: options.now ?? new PreciseDate(),
        };
    }
    preciseNow() {
        const elapsedHrTime = process.hrtime(this.#originClockTime);
        const elapsedNanoseconds = BigInt(elapsedHrTime[0]) * BigInt(1e9) + BigInt(elapsedHrTime[1]);
        const preciseDate = new PreciseDate(this.#originPreciseDate.getFullTime() + elapsedNanoseconds);
        const dateStruct = preciseDate.toStruct();
        return [dateStruct.seconds, dateStruct.nanos];
    }
    reset() {
        this._origin = {
            clockTime: process.hrtime(),
            preciseDate: new PreciseDate(),
        };
    }
}
//# sourceMappingURL=preciseWallClock.js.map