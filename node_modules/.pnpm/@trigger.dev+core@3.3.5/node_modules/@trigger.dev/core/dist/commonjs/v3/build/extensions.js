"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.esbuildPlugin = esbuildPlugin;
function esbuildPlugin(plugin, options = {}) {
    return {
        name: plugin.name,
        onBuildStart(context) {
            context.registerPlugin(plugin, options);
        },
    };
}
//# sourceMappingURL=extensions.js.map