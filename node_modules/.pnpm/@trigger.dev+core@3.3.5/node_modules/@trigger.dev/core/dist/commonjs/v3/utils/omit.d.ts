export declare function omit<T extends Record<string, any>, K extends keyof T>(obj: T, ...keys: K[]): Omit<T, K>;
