export interface StreamsProperties {
    [key: string]: string;
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
