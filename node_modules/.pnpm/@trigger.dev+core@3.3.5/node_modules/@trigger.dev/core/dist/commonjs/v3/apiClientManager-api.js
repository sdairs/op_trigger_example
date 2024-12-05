"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.apiClientManager = void 0;
// Split module-level variable definition into separate files to allow
// tree-shaking on each api instance.
const index_js_1 = require("./apiClientManager/index.js");
/** Entrypoint for logger API */
exports.apiClientManager = index_js_1.APIClientManagerAPI.getInstance();
//# sourceMappingURL=apiClientManager-api.js.map