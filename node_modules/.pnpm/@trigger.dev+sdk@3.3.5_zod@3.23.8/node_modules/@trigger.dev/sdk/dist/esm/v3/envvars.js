import { apiClientManager, isRequestOptions, mergeRequestOptions, taskContext, } from "@trigger.dev/core/v3";
import { tracer } from "./tracer.js";
export function upload(projectRefOrParams, slugOrRequestOptions, params, requestOptions) {
    let $projectRef;
    let $params;
    let $slug;
    const $requestOptions = overloadRequestOptions("upload", slugOrRequestOptions, requestOptions);
    if (taskContext.ctx) {
        if (typeof projectRefOrParams === "string") {
            $projectRef = projectRefOrParams;
            $slug =
                typeof slugOrRequestOptions === "string"
                    ? slugOrRequestOptions
                    : taskContext.ctx.environment.slug;
            if (!params) {
                throw new Error("params is required");
            }
            $params = params;
        }
        else {
            $params = projectRefOrParams;
            $projectRef = taskContext.ctx.project.ref;
            $slug = taskContext.ctx.environment.slug;
        }
    }
    else {
        if (typeof projectRefOrParams !== "string") {
            throw new Error("projectRef is required");
        }
        if (!slugOrRequestOptions || typeof slugOrRequestOptions !== "string") {
            throw new Error("slug is required");
        }
        if (!params) {
            throw new Error("params is required");
        }
        $projectRef = projectRefOrParams;
        $slug = slugOrRequestOptions;
        $params = params;
    }
    const apiClient = apiClientManager.clientOrThrow();
    return apiClient.importEnvVars($projectRef, $slug, $params, $requestOptions);
}
export function list(projectRefOrRequestOptions, slug, requestOptions) {
    const $projectRef = !isRequestOptions(projectRefOrRequestOptions)
        ? projectRefOrRequestOptions
        : taskContext.ctx?.project.ref;
    const $slug = slug ?? taskContext.ctx?.environment.slug;
    let $requestOptions = isRequestOptions(projectRefOrRequestOptions)
        ? projectRefOrRequestOptions
        : requestOptions;
    if (!$projectRef) {
        throw new Error("projectRef is required");
    }
    if (!$slug) {
        throw new Error("slug is required");
    }
    $requestOptions = mergeRequestOptions({
        tracer,
        name: "envvars.list()",
        icon: "id-badge",
    }, $requestOptions);
    const apiClient = apiClientManager.clientOrThrow();
    return apiClient.listEnvVars($projectRef, $slug, $requestOptions);
}
export function create(projectRefOrParams, slugOrRequestOptions, params, requestOptions) {
    let $projectRef;
    let $slug;
    let $params;
    const $requestOptions = overloadRequestOptions("create", slugOrRequestOptions, requestOptions);
    if (taskContext.ctx) {
        if (typeof projectRefOrParams === "string") {
            $projectRef = projectRefOrParams;
            $slug =
                typeof slugOrRequestOptions === "string"
                    ? slugOrRequestOptions
                    : taskContext.ctx.environment.slug;
            if (!params) {
                throw new Error("params is required");
            }
            $params = params;
        }
        else {
            $params = projectRefOrParams;
            $projectRef = taskContext.ctx.project.ref;
            $slug = taskContext.ctx.environment.slug;
        }
    }
    else {
        if (typeof projectRefOrParams !== "string") {
            throw new Error("projectRef is required");
        }
        if (!slugOrRequestOptions || typeof slugOrRequestOptions !== "string") {
            throw new Error("slug is required");
        }
        if (!params) {
            throw new Error("params is required");
        }
        $projectRef = projectRefOrParams;
        $slug = slugOrRequestOptions;
        $params = params;
    }
    const apiClient = apiClientManager.clientOrThrow();
    return apiClient.createEnvVar($projectRef, $slug, $params, $requestOptions);
}
export function retrieve(projectRefOrName, slugOrRequestOptions, name, requestOptions) {
    let $projectRef;
    let $slug;
    let $name;
    const $requestOptions = overloadRequestOptions("retrieve", slugOrRequestOptions, requestOptions);
    if (typeof name === "string") {
        $projectRef = projectRefOrName;
        $slug =
            typeof slugOrRequestOptions === "string"
                ? slugOrRequestOptions
                : taskContext.ctx?.environment.slug;
        $name = name;
    }
    else {
        $projectRef = taskContext.ctx?.project.ref;
        $slug = taskContext.ctx?.environment.slug;
        $name = projectRefOrName;
    }
    if (!$projectRef) {
        throw new Error("projectRef is required");
    }
    if (!$slug) {
        throw new Error("slug is required");
    }
    const apiClient = apiClientManager.clientOrThrow();
    return apiClient.retrieveEnvVar($projectRef, $slug, $name, $requestOptions);
}
export function del(projectRefOrName, slugOrRequestOptions, name, requestOptions) {
    let $projectRef;
    let $slug;
    let $name;
    const $requestOptions = overloadRequestOptions("del", slugOrRequestOptions, requestOptions);
    if (typeof name === "string") {
        $projectRef = projectRefOrName;
        $slug =
            typeof slugOrRequestOptions === "string"
                ? slugOrRequestOptions
                : taskContext.ctx?.environment.slug;
        $name = name;
    }
    else {
        $projectRef = taskContext.ctx?.project.ref;
        $slug = taskContext.ctx?.environment.slug;
        $name = projectRefOrName;
    }
    if (!$projectRef) {
        throw new Error("projectRef is required");
    }
    if (!$slug) {
        throw new Error("slug is required");
    }
    const apiClient = apiClientManager.clientOrThrow();
    return apiClient.deleteEnvVar($projectRef, $slug, $name, $requestOptions);
}
export function update(projectRefOrName, slugOrParams, nameOrRequestOptions, params, requestOptions) {
    let $projectRef;
    let $slug;
    let $name;
    let $params;
    const $requestOptions = overloadRequestOptions("update", nameOrRequestOptions, requestOptions);
    if (taskContext.ctx) {
        if (typeof slugOrParams === "string") {
            $projectRef = slugOrParams;
            $slug = slugOrParams ?? taskContext.ctx.environment.slug;
            $name =
                typeof nameOrRequestOptions === "string"
                    ? nameOrRequestOptions
                    : taskContext.ctx.environment.slug;
            if (!params) {
                throw new Error("params is required");
            }
            $params = params;
        }
        else {
            $params = slugOrParams;
            $projectRef = taskContext.ctx.project.ref;
            $slug = taskContext.ctx.environment.slug;
            $name = projectRefOrName;
        }
    }
    else {
        if (typeof slugOrParams !== "string") {
            throw new Error("slug is required");
        }
        if (!projectRefOrName) {
            throw new Error("projectRef is required");
        }
        if (!params) {
            throw new Error("params is required");
        }
        $projectRef = projectRefOrName;
        $slug = slugOrParams;
        $name = name;
        $params = params;
    }
    const apiClient = apiClientManager.clientOrThrow();
    return apiClient.updateEnvVar($projectRef, $slug, $name, $params, $requestOptions);
}
function overloadRequestOptions(name, slugOrRequestOptions, requestOptions) {
    if (isRequestOptions(slugOrRequestOptions)) {
        return mergeRequestOptions({
            tracer,
            name: `envvars.${name}()`,
            icon: "id-badge",
        }, slugOrRequestOptions);
    }
    else {
        return mergeRequestOptions({
            tracer,
            name: `envvars.${name}()`,
            icon: "id-badge",
        }, requestOptions);
    }
}
//# sourceMappingURL=envvars.js.map