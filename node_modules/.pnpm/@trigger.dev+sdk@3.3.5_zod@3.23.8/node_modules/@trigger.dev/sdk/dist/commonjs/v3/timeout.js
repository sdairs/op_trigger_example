"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.timeout = void 0;
const v3_1 = require("@trigger.dev/core/v3");
const MAXIMUM_MAX_DURATION = 2_147_483_647;
exports.timeout = {
    None: MAXIMUM_MAX_DURATION,
    signal: v3_1.timeout.signal,
};
//# sourceMappingURL=timeout.js.map