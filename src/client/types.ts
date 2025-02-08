export interface KsqlDBCommandResponse {
    statementText: string;
    commandId?: string;
    commandStatus: {
        status: 'SUCCESS' | 'ERROR' | 'PENDING' | 'TERMINATED';
        message: string;
        queryId?: string;
    };
    commandSequenceNumber: number;
    warnings: string[];
}

export interface StreamsProperties {
    [key: string]: string;
}

export interface KsqlDBRequestConfig {
    streamsProperties?: StreamsProperties;
    commandSequenceNumber?: number;
}

export type KsqlDBResponse = KsqlDBCommandResponse[];

export interface ServerInfo {
    version: string;
    kafkaClusterId: string;
    ksqlServiceId: string;
    serverStatus: 'RUNNING' | 'ERROR' | 'DEGRADED';
}
