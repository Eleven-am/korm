import { FormatConfig } from './base';
import { SelectQuery } from './select';
import { WindowSpec, WindowDuration } from './window';
import { JoinType, DataSourceType, SourceType } from './enums';

interface BaseSource<T extends SourceType> {
    type: T;
    alias?: string;
}

interface DirectSource extends BaseSource<SourceType.DIRECT> {
    name: string;
    sourceType: DataSourceType;
}

interface SubquerySource extends BaseSource<SourceType.SUBQUERY> {
    query: SelectQuery;
    alias: string;
}

export type Source = DirectSource | SubquerySource;

export interface BaseJoinCondition {
    leftField: string;
    rightField: string;
}

export interface StreamJoinWindow {
    before: WindowDuration;
    after: WindowDuration;
    gracePeriod?: WindowDuration;
}

interface BaseDataSource<T extends DataSourceType> {
    name: string;
    alias?: string;
    sourceType: T;
}

export interface StreamJoin {
    type: JoinType;
    source: BaseDataSource<DataSourceType.STREAM>;
    window: StreamJoinWindow;
    conditions: BaseJoinCondition[];
}

export interface TableJoin {
    type: JoinType;
    source: BaseDataSource<DataSourceType.TABLE>;
    conditions: BaseJoinCondition[];
}

export interface StreamTableJoin {
    type: JoinType;
    source: BaseDataSource<DataSourceType.TABLE>;
    conditions: BaseJoinCondition[];
    foreignKey?: boolean;
}

interface TimestampConfig {
    column: string;
    format?: string;
    timezone?: string;
}

interface RetentionConfig {
    size?: number;
    time?: WindowDuration
}

export interface StreamSourceOptions {
    window?: WindowSpec;
    partitionBy?: string[];
    timestamp?: TimestampConfig;
    format?: FormatConfig;
    retention?: RetentionConfig;
    emitChanges?: boolean;
    topicName?: string;
    replicas?: number;
    partitions?: number;
}

export interface TableSourceOptions {
    clusterBy?: string[];
    timestamp?: TimestampConfig;
    format?: FormatConfig;
    retention?: RetentionConfig;
    emitChanges?: boolean;
    stateStoreName?: string;
    caching?: boolean;
    topicName?: string;
    replicas?: number;
    partitions?: number;
}

interface StreamStreamJoinClause {
    sourceType: DataSourceType.STREAM;
    source: Source & { sourceType: DataSourceType.STREAM };
    sourceOptions?: StreamSourceOptions;
    joins?: StreamJoin[];
}

interface StreamTableJoinClause {
    sourceType: DataSourceType.STREAM;
    source: Source & { sourceType: DataSourceType.STREAM };
    sourceOptions?: StreamSourceOptions;
    joins: StreamTableJoin[];
}

interface TableTableJoinClause {
    sourceType: DataSourceType.TABLE;
    source: Source & { sourceType: DataSourceType.TABLE };
    sourceOptions?: TableSourceOptions;
    joins: TableJoin[];
}

interface NonJoinClause {
    sourceType: DataSourceType;
    source: Source;
    sourceOptions?: StreamSourceOptions | TableSourceOptions;
}

export type FromClause =
    | StreamStreamJoinClause
    | StreamTableJoinClause
    | TableTableJoinClause
    | NonJoinClause;

