export interface StreamsProperties {
    'ksql.streams.auto.offset.reset'?: 'earliest' | 'latest';
    'ksql.query.pull.table.scan.enabled'?: boolean;
    'ksql.query.pull.max.allowed.offset.lag'?: number;
    'processing.guarantee'?: 'at_least_once' | 'exactly_once';
    'cache.max.bytes.buffering'?: number;
    'ksql.streams.cache.max.bytes.buffering'?: number;
    'timestamp.format'?: string;
    'timestamp.extractors'?: string;
    'ksql.streams.state.dir'?: string;
    'ksql.persistent.prefix'?: string;
    'num.stream.threads'?: number;
    'commit.interval.ms'?: number;
    'ksql.streams.error.max.interval.ms'?: number;
    'ksql.streams.retry.backoff.ms'?: number;
    [key: string]: string | number | boolean | undefined;
}

export interface KsqlDBRequestConfig {
    streamsProperties?: StreamsProperties;
    commandSequenceNumber?: number;
}

export interface KsqlDBConfig {
    host: string;
    port: number;
    protocol?: 'http' | 'https';
    auth?: {
        username: string;
        password: string;
    };
    defaultStreamProperties?: StreamsProperties;
}
