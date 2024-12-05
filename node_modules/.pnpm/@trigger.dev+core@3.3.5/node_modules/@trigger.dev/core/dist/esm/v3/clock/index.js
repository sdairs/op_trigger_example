const API_NAME = "clock";
import { getGlobal, registerGlobal } from "../utils/globals.js";
import { SimpleClock } from "./simpleClock.js";
const SIMPLE_CLOCK = new SimpleClock();
export class ClockAPI {
    static _instance;
    constructor() { }
    static getInstance() {
        if (!this._instance) {
            this._instance = new ClockAPI();
        }
        return this._instance;
    }
    setGlobalClock(clock) {
        return registerGlobal(API_NAME, clock);
    }
    preciseNow() {
        return this.#getClock().preciseNow();
    }
    reset() {
        this.#getClock().reset();
    }
    #getClock() {
        return getGlobal(API_NAME) ?? SIMPLE_CLOCK;
    }
}
//# sourceMappingURL=index.js.map