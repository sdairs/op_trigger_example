"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.logger = void 0;
// Split module-level variable definition into separate files to allow
// tree-shaking on each api instance.
const index_js_1 = require("./logger/index.js");
/** Entrypoint for logger API */
exports.logger = index_js_1.LoggerAPI.getInstance();
//# sourceMappingURL=logger-api.js.map