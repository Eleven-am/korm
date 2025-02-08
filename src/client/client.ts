import { KSQLStatementBuilder } from '../builders';
import { ksqlStatementSchema } from '../schemas';
import { KSQLStatement } from '../types';
import { StreamsProperties, KsqlDBCommandResponse, ServerInfo, KsqlDBRequestConfig, KsqlDBResponse } from './types';

export class KsqlDBError extends Error {
    constructor(
        public readonly errorCode: number,
        public readonly message: string,
        public readonly statementText: string,
        public readonly entities?: { name: string; type: string; topic?: string; queryId?: string; }[]
    ) {
        super(message);
        this.name = 'KsqlDBError';
    }
}

export class ValidationError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ValidationError';
    }
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

export class KsqlDBClient {
    private readonly builder = new KSQLStatementBuilder();
    private readonly auth?: { username: string; password: string };
    private readonly defaultStreamProperties: StreamsProperties;
    private readonly baseUrl: string;

    constructor(config: KsqlDBConfig) {
        const { host, port, protocol = 'http', auth, defaultStreamProperties = {} } = config;
        this.baseUrl = `${protocol}://${host}:${port}`;
        this.auth = auth;
        this.defaultStreamProperties = defaultStreamProperties;
    }

    /**
     * Executes a KSQL statement
     * Handles DDL (CREATE/DROP) and persistent queries (CREATE ... AS SELECT)
     */
    async executeStatement(statement: KSQLStatement, config?: KsqlDBRequestConfig): Promise<KsqlDBResponse> {
        const endpoint = '/ksql';
        const payload = {
            ksql: this.getQueryString(statement),
            streamsProperties: {
                ...this.defaultStreamProperties,
                ...config?.streamsProperties
            },
            commandSequenceNumber: config?.commandSequenceNumber
        };

        return this.makeRequest<KsqlDBCommandResponse[]>(endpoint, payload);
    }

    /**
     * Executes multiple KSQL statements in sequence
     */
    async executeStatements(statements: KSQLStatement[], config?: KsqlDBRequestConfig): Promise<KsqlDBResponse> {
        const endpoint = '/ksql';
        const payload = {
            ksql: statements.map(stmt => this.getQueryString(stmt)).join(';'),
            streamsProperties: {
                ...this.defaultStreamProperties,
                ...config?.streamsProperties
            },
            commandSequenceNumber: config?.commandSequenceNumber
        };

        return this.makeRequest<KsqlDBCommandResponse[]>(endpoint, payload);
    }

    /**
     * Gets the server status and version
     */
    async getServerInfo(): Promise<ServerInfo> {
        return this.makeRequest<ServerInfo>('/info', {});
    }

    /**
     * Checks server health
     */
    async healthCheck(): Promise<boolean> {
        try {
            await this.makeRequest<{ status: string }>('/healthcheck', {});
            return true;
        } catch (error) {
            return false;
        }
    }

    private getQueryString(statement: KSQLStatement): string {
        const parsed = ksqlStatementSchema.safeParse(statement);
        if (!parsed.success) {
            throw new ValidationError(parsed.error.message);
        }

        if (!this.builder.validate(parsed.data)) {
            throw new ValidationError('Invalid KSQL statement');
        }

        return this.builder.build(parsed.data);
    }

    private async makeRequest<T>(endpoint: string, payload: unknown): Promise<T> {
        const headers: HeadersInit = {
            'Content-Type': 'application/json'
        };

        if (this.auth) {
            const authString = Buffer.from(
                `${this.auth.username}:${this.auth.password}`
            ).toString('base64');
            headers['Authorization'] = `Basic ${authString}`;
        }

        const response = await fetch(`${this.baseUrl}${endpoint}`, {
            method: 'POST',
            headers,
            body: JSON.stringify(payload)
        });

        const responseData = await response.json();

        if (!response.ok) {
            const error = responseData as KsqlDBError;
            throw new KsqlDBError(
                error.errorCode,
                error.message,
                error.statementText,
                error.entities
            );
        }

        return responseData as T;
    }
}
