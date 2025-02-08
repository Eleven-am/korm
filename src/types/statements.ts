import { FormatConfig } from './base';
import { CastItem } from './cast';
import { InsertType, QueryType, DataSourceType, PropertyAction, CreateType } from './enums';
import { LiteralExpression } from './expressions';
import { SelectQuery } from './select';

interface InsertTarget {
    name: string;
    columns?: string[];
}

interface InsertValue {
    column: string;
    value: LiteralExpression;
}

interface InsertBaseQuery<T, U extends InsertType> {
    target: InsertTarget;
    data: T;
    type: U;
    schema?: {
        fields: Array<{
            name: string;
            type: CastItem;
            required?: boolean;
        }>;
    };
}

export type InsertValueQuery = InsertBaseQuery<InsertValue[], InsertType.VALUES>;
export type InsertSelectQuery = InsertBaseQuery<SelectQuery, InsertType.SELECT>;

export interface InsertQuery {
    type: QueryType.INSERT;
    statement: InsertValueQuery | InsertSelectQuery;
}

export interface DropStatement {
    type: QueryType.DROP;
    sourceType: DataSourceType;
    sourceName: string;
    options?: {
        ifExists?: boolean;
        deleteTopic?: boolean;
    };
}

export interface TerminateQuery {
    type: QueryType.TERMINATE;
    queryId: string;
}

export interface PropertyStatement {
    type: QueryType.PROPERTY;
    action: PropertyAction;
    property: string;
    value?: string;
}

interface CreateSourceOptions {
    format: FormatConfig;
    kafkaTopic?: string;
    partitions?: number;
    replicas?: number;
    timestampColumn?: {
        name: string;
        format?: string;
    };
}

interface TableOptions extends CreateSourceOptions {
    stateStoreName?: string;
    caching?: boolean;
}

interface SchemaField {
    name: string;
    type: CastItem;
    key?: boolean;
}

export interface CreateSourceStatement<sourceType extends DataSourceType> {
    type: QueryType.CREATE;
    createType: CreateType.SOURCE;
    sourceType: sourceType;
    sourceName: string;
    ifNotExists?: boolean;
    schema: SchemaField[];
    options: sourceType extends DataSourceType.STREAM
        ? CreateSourceOptions
        : TableOptions;
}

export interface CreateAsSelectStatement<sourceType extends DataSourceType> {
    type: QueryType.CREATE;
    createType: CreateType.AS_SELECT;
    sourceType: sourceType;
    sourceName: string;
    ifNotExists?: boolean;
    select: SelectQuery;
    options: sourceType extends DataSourceType.STREAM
        ? CreateSourceOptions
        : TableOptions;
}

export type CreateStatement =
    | CreateSourceStatement<DataSourceType.STREAM>
    | CreateSourceStatement<DataSourceType.TABLE>
    | CreateAsSelectStatement<DataSourceType.STREAM>
    | CreateAsSelectStatement<DataSourceType.TABLE>;

export interface ListStatement {
    type: QueryType.LIST;
    sourceType: DataSourceType;
    extended?: boolean;
}

export enum ShowType {
    QUERIES = 'QUERIES',
    PROPERTIES = 'PROPERTIES',
    TOPICS = 'TOPICS',
    VARIABLES = 'VARIABLES'
}

export interface ShowStatement {
    type: QueryType.SHOW;
    showType: ShowType;
    extended?: boolean;
}

export interface ExplainQuery {
    type: QueryType.EXPLAIN;
    statement: SelectQuery | CreateAsSelectStatement<DataSourceType>;
    analyze?: boolean;
}

export interface DescribeQuery {
    type: QueryType.DESCRIBE;
    target: {
        type: DataSourceType;
        name: string;
    };
    extended?: boolean;
}

export type KSQLStatement =
    | SelectQuery
    | InsertQuery
    | DropStatement
    | CreateStatement
    | TerminateQuery
    | PropertyStatement
    | ListStatement
    | ShowStatement
    | ExplainQuery
    | DescribeQuery;
