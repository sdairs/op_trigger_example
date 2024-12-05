"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.iconStringForSeverity = iconStringForSeverity;
const api_logs_1 = require("@opentelemetry/api-logs");
function iconStringForSeverity(severityNumber) {
    switch (severityNumber) {
        case api_logs_1.SeverityNumber.UNSPECIFIED:
            return undefined;
        case api_logs_1.SeverityNumber.TRACE:
        case api_logs_1.SeverityNumber.TRACE2:
        case api_logs_1.SeverityNumber.TRACE3:
        case api_logs_1.SeverityNumber.TRACE4:
            return "trace";
        case api_logs_1.SeverityNumber.DEBUG:
        case api_logs_1.SeverityNumber.DEBUG2:
        case api_logs_1.SeverityNumber.DEBUG3:
        case api_logs_1.SeverityNumber.DEBUG4:
            return "debug";
        case api_logs_1.SeverityNumber.INFO:
        case api_logs_1.SeverityNumber.INFO2:
        case api_logs_1.SeverityNumber.INFO3:
        case api_logs_1.SeverityNumber.INFO4:
            return "info";
        case api_logs_1.SeverityNumber.WARN:
        case api_logs_1.SeverityNumber.WARN2:
        case api_logs_1.SeverityNumber.WARN3:
        case api_logs_1.SeverityNumber.WARN4:
            return "warn";
        case api_logs_1.SeverityNumber.ERROR:
        case api_logs_1.SeverityNumber.ERROR2:
        case api_logs_1.SeverityNumber.ERROR3:
        case api_logs_1.SeverityNumber.ERROR4:
            return "error";
        case api_logs_1.SeverityNumber.FATAL:
        case api_logs_1.SeverityNumber.FATAL2:
        case api_logs_1.SeverityNumber.FATAL3:
        case api_logs_1.SeverityNumber.FATAL4:
            return "fatal";
    }
}
//# sourceMappingURL=icons.js.map