import { flattenAttributes } from "./flattenAttributes.js";
import { SemanticInternalAttributes } from "../semanticInternalAttributes.js";
export function accessoryAttributes(accessory) {
    return flattenAttributes(accessory, SemanticInternalAttributes.STYLE_ACCESSORY);
}
//# sourceMappingURL=styleAttributes.js.map