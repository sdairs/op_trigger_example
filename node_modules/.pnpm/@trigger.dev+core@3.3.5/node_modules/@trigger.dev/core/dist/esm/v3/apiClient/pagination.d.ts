export interface CursorPageParams {
    limit?: number;
    after?: string;
    before?: string;
}
export interface OffsetLimitPageParams {
    limit?: number;
    page?: number;
}
export interface PageResponse<Item> {
    data: Array<Item>;
}
export interface CursorPageResponse<Item> extends PageResponse<Item> {
    pagination: {
        next?: string;
        previous?: string;
    };
}
export interface OffsetLimitPageResponse<Item> extends PageResponse<Item> {
    pagination: {
        currentPage: number;
        totalPages: number;
        count: number;
    };
}
export interface Page<Item> {
    getPaginatedItems(): Item[];
    hasNextPage(): boolean;
    hasPreviousPage(): boolean;
}
export declare class CursorPage<Item> implements CursorPageResponse<Item>, Page<Item>, AsyncIterable<Item> {
    private pageFetcher;
    data: Array<Item>;
    pagination: {
        next?: string;
        previous?: string;
    };
    constructor(data: Array<Item>, pagination: {
        next?: string;
        previous?: string;
    }, pageFetcher: (params: Omit<CursorPageParams, "limit">) => Promise<CursorPage<Item>>);
    getPaginatedItems(): Item[];
    hasNextPage(): boolean;
    hasPreviousPage(): boolean;
    getNextPage(): Promise<CursorPage<Item>>;
    getPreviousPage(): Promise<CursorPage<Item>>;
    iterPages(): AsyncGenerator<CursorPage<Item>, void, unknown>;
    [Symbol.asyncIterator](): AsyncGenerator<Awaited<Item>, void, unknown>;
}
export declare class OffsetLimitPage<Item> implements OffsetLimitPageResponse<Item>, Page<Item>, AsyncIterable<Item> {
    private pageFetcher;
    data: Array<Item>;
    pagination: {
        currentPage: number;
        totalPages: number;
        count: number;
    };
    constructor(data: Array<Item>, pagination: {
        currentPage: number;
        totalPages: number;
        count: number;
    }, pageFetcher: (params: Omit<OffsetLimitPageParams, "limit">) => Promise<OffsetLimitPage<Item>>);
    getPaginatedItems(): Item[];
    hasNextPage(): boolean;
    hasPreviousPage(): boolean;
    getNextPage(): Promise<OffsetLimitPage<Item>>;
    getPreviousPage(): Promise<OffsetLimitPage<Item>>;
    iterPages(): AsyncGenerator<OffsetLimitPage<Item>, void, unknown>;
    [Symbol.asyncIterator](): AsyncGenerator<Awaited<Item>, void, unknown>;
}
