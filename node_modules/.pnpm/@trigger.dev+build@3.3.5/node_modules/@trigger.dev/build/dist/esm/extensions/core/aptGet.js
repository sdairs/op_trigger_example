export function aptGet(options) {
    return {
        name: "aptGet",
        onBuildComplete(context) {
            if (context.target === "dev") {
                return;
            }
            context.logger.debug("Adding apt-get layer", {
                pkgs: options.packages,
            });
            context.addLayer({
                id: "apt-get",
                image: {
                    pkgs: options.packages,
                },
            });
        },
    };
}
//# sourceMappingURL=aptGet.js.map