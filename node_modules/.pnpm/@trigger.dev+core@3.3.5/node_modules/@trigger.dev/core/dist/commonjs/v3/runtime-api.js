"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.runtime = void 0;
// Split module-level variable definition into separate files to allow
// tree-shaking on each api instance.
const index_js_1 = require("./runtime/index.js");
/** Entrypoint for runtime API */
exports.runtime = index_js_1.RuntimeAPI.getInstance();
//# sourceMappingURL=runtime-api.js.map