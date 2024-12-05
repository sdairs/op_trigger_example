export function omit(obj, ...keys) {
    const result = {};
    for (const key in obj) {
        if (!keys.includes(key)) {
            result[key] = obj[key];
        }
    }
    return result;
}
//# sourceMappingURL=omit.js.map