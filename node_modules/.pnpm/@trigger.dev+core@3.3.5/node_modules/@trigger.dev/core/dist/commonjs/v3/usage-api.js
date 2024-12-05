"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.usage = void 0;
// Split module-level variable definition into separate files to allow
// tree-shaking on each api instance.
const api_js_1 = require("./usage/api.js");
/** Entrypoint for usage API */
exports.usage = api_js_1.UsageAPI.getInstance();
//# sourceMappingURL=usage-api.js.map