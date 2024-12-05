"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SimpleClock = void 0;
const precise_date_1 = require("@google-cloud/precise-date");
class SimpleClock {
    preciseNow() {
        const now = new precise_date_1.PreciseDate();
        const nowStruct = now.toStruct();
        return [nowStruct.seconds, nowStruct.nanos];
    }
    reset() {
        // do nothing
    }
}
exports.SimpleClock = SimpleClock;
//# sourceMappingURL=simpleClock.js.map