"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.taskContext = void 0;
// Split module-level variable definition into separate files to allow
// tree-shaking on each api instance.
const index_js_1 = require("./taskContext/index.js");
/** Entrypoint for logger API */
exports.taskContext = index_js_1.TaskContextAPI.getInstance();
//# sourceMappingURL=task-context-api.js.map