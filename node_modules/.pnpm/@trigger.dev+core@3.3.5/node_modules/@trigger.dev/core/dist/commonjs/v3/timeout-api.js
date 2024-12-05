"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.timeout = void 0;
// Split module-level variable definition into separate files to allow
// tree-shaking on each api instance.
const api_js_1 = require("./timeout/api.js");
/** Entrypoint for timeout API */
exports.timeout = api_js_1.TimeoutAPI.getInstance();
//# sourceMappingURL=timeout-api.js.map