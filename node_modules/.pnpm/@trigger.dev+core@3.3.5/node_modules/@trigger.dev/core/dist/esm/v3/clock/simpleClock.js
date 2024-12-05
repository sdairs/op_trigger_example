import { PreciseDate } from "@google-cloud/precise-date";
export class SimpleClock {
    preciseNow() {
        const now = new PreciseDate();
        const nowStruct = now.toStruct();
        return [nowStruct.seconds, nowStruct.nanos];
    }
    reset() {
        // do nothing
    }
}
//# sourceMappingURL=simpleClock.js.map