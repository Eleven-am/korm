import { z } from 'zod';

const commandStatusSchema = z.object({
    status: z.enum(['SUCCESS', 'ERROR', 'PENDING', 'TERMINATED']),
    message: z.string(),
    queryId: z.string().optional(),
});

export const ksqlDBCommandResponseSchema = z.object({
    statementText: z.string(),
    commandId: z.string().optional(),
    commandStatus: commandStatusSchema,
    commandSequenceNumber: z.number(),
    warnings: z.array(z.string()),
});

export const ksqlDBCommandResponseArraySchema = z.array(ksqlDBCommandResponseSchema);

export const healthCheckSchema = z.object({
    isHealthy: z.boolean(),
    details: z.record(z.any()).optional()
})

export const streamMetricsSchema = z.object({
    status: z.string(),
    bytesConsumed: z.number(),
    messagesConsumed: z.number(),
    totalDuration: z.number(),
    rowsProduced: z.number(),
    lastMessage: z.string().optional(),
    errorCount: z.number(),
    queryErrors: z.array(z.object({
        message: z.string(),
        timestamp: z.string(),
    })).optional(),
    lagMetrics: z.array(z.object({
        topicPartition: z.string(),
        lag: z.number(),
        timestamp: z.string(),
    })).optional(),
});

export const ksqlDBErrorSchema = z.object({
    errorCode: z.number(),
    message: z.string(),
    statementText: z.string().optional(),
    entities: z.array(z.string()).optional(),
});

export const streamStatusSchema = z.object({
    queryString: z.string(),
    state: z.enum(['RUNNING', 'PAUSED', 'ERROR', 'TERMINATED']),
    statusMessage: z.string().optional(),
    sinkTopic: z.string().optional(),
    sourceTopic: z.string().optional(),
});

export const streamPropertiesSchema = z.object({
    properties: z.record(z.union([z.string(), z.number(), z.boolean()])),
    overriddenProperties: z.array(z.string()),
    defaultProperties: z.record(z.string()),
});

export const topicDescriptionSchema = z.object({
    name: z.string(),
    partitions: z.number(),
    replicationFactor: z.number(),
    configs: z.record(z.string()),
});

export const queryDescriptionSchema = z.object({
    queryString: z.string(),
    sinks: z.array(z.string()),
    sources: z.array(z.string()),
    fields: z.array(z.object({
        name: z.string(),
        type: z.string(),
    })),
    runtime: z.object({
        processedMessages: z.number(),
        processedBytes: z.number(),
        totalDuration: z.number(),
    }).optional(),
});

export const schemaDescriptionSchema = z.object({
    type: z.string(),
    fields: z.array(z.object({
        name: z.string(),
        schema: z.object({
            type: z.string(),
            fields: z.array(z.object({
                name: z.string(),
                type: z.string(),
                optional: z.boolean().optional(),
            })).optional(),
        }),
    })),
});

export const pullQueryRowSchema = z.object({
    row: z.record(z.any()),
    timestamp: z.number().optional(),
});

export const pullQueryResponseSchema = z.object({
    queryId: z.string(),
    columnNames: z.array(z.string()),
    columnTypes: z.array(z.string()),
    rows: z.array(pullQueryRowSchema),
});

export const pushQueryResponseSchema = z.object({
    queryId: z.string(),
    columnNames: z.array(z.string()),
    columnTypes: z.array(z.string()),
    rows: z.array(z.array(z.any())),
});

export const connectorDescriptionSchema = z.object({
    name: z.string(),
    type: z.string(),
    className: z.string(),
    state: z.string(),
    tasks: z.array(z.object({
        taskId: z.number(),
        state: z.string(),
        trace: z.string().optional(),
    })),
    config: z.record(z.string()),
});

export const connectorListSchema = z.array(z.object({
    name: z.string(),
    type: z.string(),
    state: z.string(),
}));

export const queryValidationSchema = z.object({
    queryId: z.string().optional(),
    kafka_topics: z.array(z.string()),
    warnings: z.array(z.string()),
    valid: z.boolean(),
    message: z.string().optional(),
});

export const queryMetricsSchema = z.object({
    host: z.string(),
    timestamp: z.string(),
    status: z.enum(['UP', 'DOWN', 'WARNING', 'ERROR']),
    metrics: z.object({
        consumedBytes: z.number(),
        consumedMessages: z.number(),
        lastMessageTimestamp: z.string().optional(),
        lastConsumerErrorTimestamp: z.string().optional(),
        errorRate: z.number(),
        processingRate: z.number(),
        totalMessages: z.number(),
    })
});

export const serverInfoSchema = z.object({
    version: z.string(),
    kafkaClusterId: z.string(),
    ksqlServiceId: z.string(),
    serverStatus: z.enum(['RUNNING', 'ERROR', 'SHUTTING_DOWN']),
    errorCode: z.number().optional(),
    message: z.string().optional()
})
