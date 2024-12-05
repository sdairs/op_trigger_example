"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ClockAPI = void 0;
const API_NAME = "clock";
const globals_js_1 = require("../utils/globals.js");
const simpleClock_js_1 = require("./simpleClock.js");
const SIMPLE_CLOCK = new simpleClock_js_1.SimpleClock();
class ClockAPI {
    static _instance;
    constructor() { }
    static getInstance() {
        if (!this._instance) {
            this._instance = new ClockAPI();
        }
        return this._instance;
    }
    setGlobalClock(clock) {
        return (0, globals_js_1.registerGlobal)(API_NAME, clock);
    }
    preciseNow() {
        return this.#getClock().preciseNow();
    }
    reset() {
        this.#getClock().reset();
    }
    #getClock() {
        return (0, globals_js_1.getGlobal)(API_NAME) ?? SIMPLE_CLOCK;
    }
}
exports.ClockAPI = ClockAPI;
//# sourceMappingURL=index.js.map