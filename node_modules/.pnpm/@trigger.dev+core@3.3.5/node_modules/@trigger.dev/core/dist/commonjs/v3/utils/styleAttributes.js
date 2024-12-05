"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.accessoryAttributes = accessoryAttributes;
const flattenAttributes_js_1 = require("./flattenAttributes.js");
const semanticInternalAttributes_js_1 = require("../semanticInternalAttributes.js");
function accessoryAttributes(accessory) {
    return (0, flattenAttributes_js_1.flattenAttributes)(accessory, semanticInternalAttributes_js_1.SemanticInternalAttributes.STYLE_ACCESSORY);
}
//# sourceMappingURL=styleAttributes.js.map