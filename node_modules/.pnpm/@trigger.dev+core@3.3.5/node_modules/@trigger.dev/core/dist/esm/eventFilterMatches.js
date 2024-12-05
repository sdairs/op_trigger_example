// EventFilter is a recursive type, where the keys are strings and the values are an array of strings, numbers, booleans, or objects.
// If the values of the array are strings, numbers, or booleans, than we are matching against the value of the payload.
// If the values of the array are objects, then we are doing content filtering
// An example would be [{ $endsWith: ".png" }, { $startsWith: "images/" } ]
export function eventFilterMatches(payload, filter) {
    if (payload === undefined || payload === null) {
        if (Object.entries(filter).length === 0) {
            return true;
        }
        else {
            return false;
        }
    }
    for (const [patternKey, patternValue] of Object.entries(filter)) {
        const payloadValue = payload[patternKey];
        if (Array.isArray(patternValue)) {
            if (patternValue.length === 0) {
                continue;
            }
            // Check to see if all the items in the array are a string
            if (patternValue.every((item) => typeof item === "string")) {
                if (patternValue.includes(payloadValue)) {
                    continue;
                }
                return false;
            }
            // Check to see if all the items in the array are a number
            if (patternValue.every((item) => typeof item === "number")) {
                if (patternValue.includes(payloadValue)) {
                    continue;
                }
                return false;
            }
            // Check to see if all the items in the array are a boolean
            if (patternValue.every((item) => typeof item === "boolean")) {
                if (patternValue.includes(payloadValue)) {
                    continue;
                }
                return false;
            }
            // Now we know that all the items in the array are objects
            const objectArray = patternValue;
            if (!contentFiltersMatches(payloadValue, objectArray)) {
                return false;
            }
            continue;
        }
        else if (typeof patternValue === "object") {
            if (Array.isArray(payloadValue)) {
                if (!payloadValue.some((item) => eventFilterMatches(item, patternValue))) {
                    return false;
                }
            }
            else {
                if (!eventFilterMatches(payloadValue, patternValue)) {
                    return false;
                }
            }
        }
    }
    return true;
}
function contentFiltersMatches(actualValue, contentFilters) {
    for (const contentFilter of contentFilters) {
        if (typeof contentFilter === "object") {
            if (!contentFilterMatches(actualValue, contentFilter)) {
                return false;
            }
        }
    }
    return true;
}
function contentFilterMatches(actualValue, contentFilter) {
    if ("$endsWith" in contentFilter) {
        if (typeof actualValue !== "string") {
            return false;
        }
        return actualValue.endsWith(contentFilter.$endsWith);
    }
    if ("$startsWith" in contentFilter) {
        if (typeof actualValue !== "string") {
            return false;
        }
        return actualValue.startsWith(contentFilter.$startsWith);
    }
    if ("$anythingBut" in contentFilter) {
        if (Array.isArray(contentFilter.$anythingBut)) {
            if (contentFilter.$anythingBut.includes(actualValue)) {
                return false;
            }
        }
        if (contentFilter.$anythingBut === actualValue) {
            return false;
        }
        return true;
    }
    if ("$exists" in contentFilter) {
        if (contentFilter.$exists) {
            return actualValue !== undefined;
        }
        return actualValue === undefined;
    }
    if ("$gt" in contentFilter) {
        if (typeof actualValue !== "number") {
            return false;
        }
        return actualValue > contentFilter.$gt;
    }
    if ("$lt" in contentFilter) {
        if (typeof actualValue !== "number") {
            return false;
        }
        return actualValue < contentFilter.$lt;
    }
    if ("$gte" in contentFilter) {
        if (typeof actualValue !== "number") {
            return false;
        }
        return actualValue >= contentFilter.$gte;
    }
    if ("$lte" in contentFilter) {
        if (typeof actualValue !== "number") {
            return false;
        }
        return actualValue <= contentFilter.$lte;
    }
    if ("$between" in contentFilter) {
        if (typeof actualValue !== "number") {
            return false;
        }
        return actualValue >= contentFilter.$between[0] && actualValue <= contentFilter.$between[1];
    }
    if ("$includes" in contentFilter) {
        if (Array.isArray(actualValue)) {
            return actualValue.includes(contentFilter.$includes);
        }
        return false;
    }
    // Use localCompare
    if ("$ignoreCaseEquals" in contentFilter) {
        if (typeof actualValue !== "string") {
            return false;
        }
        return (actualValue.localeCompare(contentFilter.$ignoreCaseEquals, undefined, {
            sensitivity: "accent",
        }) === 0);
    }
    if ("$isNull" in contentFilter) {
        if (contentFilter.$isNull) {
            return actualValue === null;
        }
        return actualValue !== null;
    }
    if ("$not" in contentFilter) {
        if (Array.isArray(actualValue)) {
            return !actualValue.includes(contentFilter.$not);
        }
        else if (typeof actualValue === "number" ||
            typeof actualValue === "boolean" ||
            typeof actualValue === "string") {
            return actualValue !== contentFilter.$not;
        }
        return false;
    }
    return true;
}
//# sourceMappingURL=eventFilterMatches.js.map