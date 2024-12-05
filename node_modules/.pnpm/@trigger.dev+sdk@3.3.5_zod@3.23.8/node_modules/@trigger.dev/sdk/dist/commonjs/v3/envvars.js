"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.upload = upload;
exports.list = list;
exports.create = create;
exports.retrieve = retrieve;
exports.del = del;
exports.update = update;
const v3_1 = require("@trigger.dev/core/v3");
const tracer_js_1 = require("./tracer.js");
function upload(projectRefOrParams, slugOrRequestOptions, params, requestOptions) {
    let $projectRef;
    let $params;
    let $slug;
    const $requestOptions = overloadRequestOptions("upload", slugOrRequestOptions, requestOptions);
    if (v3_1.taskContext.ctx) {
        if (typeof projectRefOrParams === "string") {
            $projectRef = projectRefOrParams;
            $slug =
                typeof slugOrRequestOptions === "string"
                    ? slugOrRequestOptions
                    : v3_1.taskContext.ctx.environment.slug;
            if (!params) {
                throw new Error("params is required");
            }
            $params = params;
        }
        else {
            $params = projectRefOrParams;
            $projectRef = v3_1.taskContext.ctx.project.ref;
            $slug = v3_1.taskContext.ctx.environment.slug;
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
    const apiClient = v3_1.apiClientManager.clientOrThrow();
    return apiClient.importEnvVars($projectRef, $slug, $params, $requestOptions);
}
function list(projectRefOrRequestOptions, slug, requestOptions) {
    const $projectRef = !(0, v3_1.isRequestOptions)(projectRefOrRequestOptions)
        ? projectRefOrRequestOptions
        : v3_1.taskContext.ctx?.project.ref;
    const $slug = slug ?? v3_1.taskContext.ctx?.environment.slug;
    let $requestOptions = (0, v3_1.isRequestOptions)(projectRefOrRequestOptions)
        ? projectRefOrRequestOptions
        : requestOptions;
    if (!$projectRef) {
        throw new Error("projectRef is required");
    }
    if (!$slug) {
        throw new Error("slug is required");
    }
    $requestOptions = (0, v3_1.mergeRequestOptions)({
        tracer: tracer_js_1.tracer,
        name: "envvars.list()",
        icon: "id-badge",
    }, $requestOptions);
    const apiClient = v3_1.apiClientManager.clientOrThrow();
    return apiClient.listEnvVars($projectRef, $slug, $requestOptions);
}
function create(projectRefOrParams, slugOrRequestOptions, params, requestOptions) {
    let $projectRef;
    let $slug;
    let $params;
    const $requestOptions = overloadRequestOptions("create", slugOrRequestOptions, requestOptions);
    if (v3_1.taskContext.ctx) {
        if (typeof projectRefOrParams === "string") {
            $projectRef = projectRefOrParams;
            $slug =
                typeof slugOrRequestOptions === "string"
                    ? slugOrRequestOptions
                    : v3_1.taskContext.ctx.environment.slug;
            if (!params) {
                throw new Error("params is required");
            }
            $params = params;
        }
        else {
            $params = projectRefOrParams;
            $projectRef = v3_1.taskContext.ctx.project.ref;
            $slug = v3_1.taskContext.ctx.environment.slug;
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
    const apiClient = v3_1.apiClientManager.clientOrThrow();
    return apiClient.createEnvVar($projectRef, $slug, $params, $requestOptions);
}
function retrieve(projectRefOrName, slugOrRequestOptions, name, requestOptions) {
    let $projectRef;
    let $slug;
    let $name;
    const $requestOptions = overloadRequestOptions("retrieve", slugOrRequestOptions, requestOptions);
    if (typeof name === "string") {
        $projectRef = projectRefOrName;
        $slug =
            typeof slugOrRequestOptions === "string"
                ? slugOrRequestOptions
                : v3_1.taskContext.ctx?.environment.slug;
        $name = name;
    }
    else {
        $projectRef = v3_1.taskContext.ctx?.project.ref;
        $slug = v3_1.taskContext.ctx?.environment.slug;
        $name = projectRefOrName;
    }
    if (!$projectRef) {
        throw new Error("projectRef is required");
    }
    if (!$slug) {
        throw new Error("slug is required");
    }
    const apiClient = v3_1.apiClientManager.clientOrThrow();
    return apiClient.retrieveEnvVar($projectRef, $slug, $name, $requestOptions);
}
function del(projectRefOrName, slugOrRequestOptions, name, requestOptions) {
    let $projectRef;
    let $slug;
    let $name;
    const $requestOptions = overloadRequestOptions("del", slugOrRequestOptions, requestOptions);
    if (typeof name === "string") {
        $projectRef = projectRefOrName;
        $slug =
            typeof slugOrRequestOptions === "string"
                ? slugOrRequestOptions
                : v3_1.taskContext.ctx?.environment.slug;
        $name = name;
    }
    else {
        $projectRef = v3_1.taskContext.ctx?.project.ref;
        $slug = v3_1.taskContext.ctx?.environment.slug;
        $name = projectRefOrName;
    }
    if (!$projectRef) {
        throw new Error("projectRef is required");
    }
    if (!$slug) {
        throw new Error("slug is required");
    }
    const apiClient = v3_1.apiClientManager.clientOrThrow();
    return apiClient.deleteEnvVar($projectRef, $slug, $name, $requestOptions);
}
function update(projectRefOrName, slugOrParams, nameOrRequestOptions, params, requestOptions) {
    let $projectRef;
    let $slug;
    let $name;
    let $params;
    const $requestOptions = overloadRequestOptions("update", nameOrRequestOptions, requestOptions);
    if (v3_1.taskContext.ctx) {
        if (typeof slugOrParams === "string") {
            $projectRef = slugOrParams;
            $slug = slugOrParams ?? v3_1.taskContext.ctx.environment.slug;
            $name =
                typeof nameOrRequestOptions === "string"
                    ? nameOrRequestOptions
                    : v3_1.taskContext.ctx.environment.slug;
            if (!params) {
                throw new Error("params is required");
            }
            $params = params;
        }
        else {
            $params = slugOrParams;
            $projectRef = v3_1.taskContext.ctx.project.ref;
            $slug = v3_1.taskContext.ctx.environment.slug;
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
    const apiClient = v3_1.apiClientManager.clientOrThrow();
    return apiClient.updateEnvVar($projectRef, $slug, $name, $params, $requestOptions);
}
function overloadRequestOptions(name, slugOrRequestOptions, requestOptions) {
    if ((0, v3_1.isRequestOptions)(slugOrRequestOptions)) {
        return (0, v3_1.mergeRequestOptions)({
            tracer: tracer_js_1.tracer,
            name: `envvars.${name}()`,
            icon: "id-badge",
        }, slugOrRequestOptions);
    }
    else {
        return (0, v3_1.mergeRequestOptions)({
            tracer: tracer_js_1.tracer,
            name: `envvars.${name}()`,
            icon: "id-badge",
        }, requestOptions);
    }
}
//# sourceMappingURL=envvars.js.map