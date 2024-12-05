import { apiClientManager } from "../apiClientManager-api.js";
export class UsageClient {
    url;
    jwt;
    constructor(url, jwt) {
        this.url = url;
        this.jwt = jwt;
    }
    async sendUsageEvent(event) {
        try {
            const response = await fetch(this.url, {
                method: "POST",
                body: JSON.stringify(event),
                headers: {
                    "content-type": "application/json",
                    "x-trigger-jwt": this.jwt,
                    accept: "application/json",
                    authorization: `Bearer ${apiClientManager.accessToken}`, // this is used to renew the JWT
                },
            });
            if (response.ok) {
                const renewedJwt = response.headers.get("x-trigger-jwt");
                if (renewedJwt) {
                    this.jwt = renewedJwt;
                }
            }
        }
        catch (error) {
            console.error(`Failed to send usage event: ${error}`);
        }
    }
}
//# sourceMappingURL=usageClient.js.map