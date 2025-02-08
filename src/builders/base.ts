export interface SQLBuilder<T> {
    build(item: T): string;
    validate(item: T): string | null;
}
