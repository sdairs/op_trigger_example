"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.clock = void 0;
// Split module-level variable definition into separate files to allow
// tree-shaking on each api instance.
const index_js_1 = require("./clock/index.js");
/** Entrypoint for clock API */
exports.clock = index_js_1.ClockAPI.getInstance();
//# sourceMappingURL=clock-api.js.map