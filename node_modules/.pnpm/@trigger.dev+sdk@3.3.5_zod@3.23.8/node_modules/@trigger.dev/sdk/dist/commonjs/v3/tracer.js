"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.tracer = void 0;
const tracer_1 = require("@trigger.dev/core/v3/tracer");
const version_js_1 = require("../version.js");
exports.tracer = new tracer_1.TriggerTracer({ name: "@trigger.dev/sdk", version: version_js_1.VERSION });
//# sourceMappingURL=tracer.js.map