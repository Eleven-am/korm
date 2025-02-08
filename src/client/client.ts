import { KSQLStatementBuilder } from '../builders';
import { ksqlStatementSchema } from '../schemas';
import { KSQLStatement } from '../types';

import { z } from 'zod';
import {
    queryValidationSchema,
    ksqlDBCommandResponseArraySchema,
    queryMetricsSchema,
    serverInfoSchema, healthCheckSchema,
} from './schemas';
import { KsqlDBConfig, StreamsProperties, KsqlDBRequestConfig } from './types';

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
     * Validates a KSQL query
     * @param query The KSQL query to validate
     */
    async validateQuery(query: string) {
        const response = await this.makeRequest(
            '/ksql/validate',
            { ksql: query },
            queryValidationSchema
        );

        if (!response.valid) {
            throw new ValidationError(response.message || 'Invalid KSQL query');
        }
    }

    /**
     * Creates a new KSQL stream
     * @param query The KSQL query to create the stream
     */
    async createStream(query: string) {
        const response = await this.makeRequest(
            '/ksql',
            {
                ksql: query,
                streamsProperties: {
                    ...this.defaultStreamProperties,
                    'ksql.streams.auto.offset.reset': 'earliest',
                    'ksql.query.pull.table.scan.enabled': 'true'
                }
            },
            ksqlDBCommandResponseArraySchema
        );

        const commandResponse = response[0];
        if (!commandResponse || commandResponse.commandStatus.status !== 'SUCCESS') {
            throw new Error(
                commandResponse?.commandStatus.message || 'Failed to create KSQL stream'
            );
        }

        const queryId = commandResponse.commandStatus.queryId;
        if (!queryId) {
            throw new Error('Stream created but no query ID returned');
        }

        return queryId;
    }

    /**
     * Updates an existing KSQL stream
     * @param pipelineId The ID of the pipeline to update
     * @param query The new KSQL query
     */
    async updateStream(pipelineId: string, query: string) {
        await this.terminateStream(pipelineId);
        return this.createStream(query);
    }

    /**
     * Pauses a KSQL stream
     * @param queryId The ID of the stream to pause
     */
    async pauseStream(queryId: string) {
        const response = await this.makeRequest(
            '/ksql',
            {
                ksql: `PAUSE ${queryId};`
            },
            ksqlDBCommandResponseArraySchema
        );

        const commandResponse = response[0];
        if (!commandResponse || commandResponse.commandStatus.status !== 'SUCCESS') {
            throw new Error(
                commandResponse?.commandStatus.message ||
                `Failed to pause stream with queryId: ${queryId}`
            );
        }
    }

    /**
     * Resumes a KSQL stream
     * @param queryId The ID of the stream to resume
     */
    async resumeStream(queryId: string) {
        const response = await this.makeRequest(
            '/ksql',
            {
                ksql: `RESUME ${queryId};`
            },
            ksqlDBCommandResponseArraySchema
        );

        const commandResponse = response[0];
        if (!commandResponse || commandResponse.commandStatus.status !== 'SUCCESS') {
            throw new Error(
                commandResponse?.commandStatus.message ||
                `Failed to resume stream with queryId: ${queryId}`
            );
        }
    }

    /**
     * Gets stream metrics
     * @param queryId The ID of the stream to get metrics for
     */
    async getStreamMetrics(queryId: string) {
        return this.makeRequest(
            `/queries/${queryId}/status`,
            {},
            queryMetricsSchema
        );
    }

    /**
     * Terminates a KSQL stream
     * @param queryId The ID of the stream to terminate
     */
    async terminateStream(queryId: string) {
        const response = await this.makeRequest(
            '/ksql/terminate',
            { queryId },
            ksqlDBCommandResponseArraySchema
        );

        const commandResponse = response[0];
        if (!commandResponse || commandResponse.commandStatus.status !== 'SUCCESS') {
            throw new Error(
                commandResponse?.commandStatus.message ||
                `Failed to terminate stream with queryId: ${queryId}`
            );
        }
    }

    /**
     * Executes a KSQL statement
     * Handles DDL (CREATE/DROP) and persistent queries (CREATE ... AS SELECT)
     * @param statement The KSQL statement to
     * @param config Optional request configuration
     */
    async executeStatement(statement: KSQLStatement, config?: KsqlDBRequestConfig) {
        const endpoint = '/ksql';
        const payload = {
            ksql: this.getQueryString(statement),
            streamsProperties: {
                ...this.defaultStreamProperties,
                ...config?.streamsProperties
            },
            commandSequenceNumber: config?.commandSequenceNumber
        };

        return this.makeRequest(
            endpoint,
            payload,
            ksqlDBCommandResponseArraySchema
        );
    }

    /**
     * Executes multiple KSQL statements in sequence
     * @param statements The KSQL statements to execute
     * @param config Optional request configuration
     */
    async executeStatements(statements: KSQLStatement[], config?: KsqlDBRequestConfig) {
        const payload = {
            ksql: statements.map(stmt => this.getQueryString(stmt)).join(';'),
            streamsProperties: {
                ...this.defaultStreamProperties,
                ...config?.streamsProperties
            },
            commandSequenceNumber: config?.commandSequenceNumber
        };

        return this.makeRequest(
            '/ksql',
            payload,
            ksqlDBCommandResponseArraySchema
        );
    }

    /**
     * Gets the server status and version
     */
    async getServerInfo() {
        return this.makeRequest('/info', {}, serverInfoSchema);
    }

    /**
     * Checks server health
     */
    async healthCheck(): Promise<boolean> {
        try {
            await this.makeRequest('/healthcheck', {}, healthCheckSchema);
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

    private async makeRequest<T>(endpoint: string, payload: unknown, schema: z.ZodType<T>): Promise<T> {
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

        const parsed = schema.safeParse(responseData);

        if (!parsed.success) {
            throw new ValidationError(parsed.error.message);
        }

        return parsed.data;
    }
}
