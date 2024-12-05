export function esbuildPlugin(plugin, options = {}) {
    return {
        name: plugin.name,
        onBuildStart(context) {
            context.registerPlugin(plugin, options);
        },
    };
}
//# sourceMappingURL=extensions.js.map