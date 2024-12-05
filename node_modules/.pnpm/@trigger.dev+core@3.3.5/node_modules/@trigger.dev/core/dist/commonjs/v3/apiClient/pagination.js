"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.OffsetLimitPage = exports.CursorPage = void 0;
class CursorPage {
    pageFetcher;
    data;
    pagination;
    constructor(data, pagination, pageFetcher) {
        this.pageFetcher = pageFetcher;
        this.data = data;
        this.pagination = pagination;
    }
    getPaginatedItems() {
        return this.data ?? [];
    }
    hasNextPage() {
        return !!this.pagination.next;
    }
    hasPreviousPage() {
        return !!this.pagination.previous;
    }
    getNextPage() {
        if (!this.pagination.next) {
            throw new Error("No next page available");
        }
        return this.pageFetcher({ after: this.pagination.next });
    }
    getPreviousPage() {
        if (!this.pagination.previous) {
            throw new Error("No previous page available");
        }
        return this.pageFetcher({ before: this.pagination.previous });
    }
    async *iterPages() {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        let page = this;
        yield page;
        while (page.hasNextPage()) {
            page = await page.getNextPage();
            yield page;
        }
    }
    async *[Symbol.asyncIterator]() {
        for await (const page of this.iterPages()) {
            for (const item of page.getPaginatedItems()) {
                yield item;
            }
        }
    }
}
exports.CursorPage = CursorPage;
class OffsetLimitPage {
    pageFetcher;
    data;
    pagination;
    constructor(data, pagination, pageFetcher) {
        this.pageFetcher = pageFetcher;
        this.data = data;
        this.pagination = pagination;
    }
    getPaginatedItems() {
        return this.data ?? [];
    }
    hasNextPage() {
        return this.pagination.currentPage < this.pagination.totalPages;
    }
    hasPreviousPage() {
        return this.pagination.currentPage > 1;
    }
    getNextPage() {
        if (!this.hasNextPage()) {
            throw new Error("No next page available");
        }
        return this.pageFetcher({
            page: this.pagination.currentPage + 1,
        });
    }
    getPreviousPage() {
        if (!this.hasPreviousPage()) {
            throw new Error("No previous page available");
        }
        return this.pageFetcher({
            page: this.pagination.currentPage - 1,
        });
    }
    async *iterPages() {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        let page = this;
        yield page;
        while (page.hasNextPage()) {
            page = await page.getNextPage();
            yield page;
        }
    }
    async *[Symbol.asyncIterator]() {
        for await (const page of this.iterPages()) {
            for (const item of page.getPaginatedItems()) {
                yield item;
            }
        }
    }
}
exports.OffsetLimitPage = OffsetLimitPage;
//# sourceMappingURL=pagination.js.map