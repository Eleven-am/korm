import { z } from 'zod';
import { SourceType, Source, DataSourceType, FromClause } from '../types';
import { formatConfigSchema } from './baseSchema';
import { selectQuerySchema } from './selectSchema';
import { windowSpecSchema, windowDurationSchema } from './windowSchema';

function baseSource<T extends SourceType>(type: T) {
    return z.object({
        type: z.literal(type),
        alias: z.string().optional(),
    })
}

export const directSourceSchema = baseSource(SourceType.DIRECT).merge(z.object({
    name: z.string(),
    sourceType: z.nativeEnum(DataSourceType),
}))

export const subquerySourceSchema = baseSource(SourceType.SUBQUERY).merge(z.object({
    query: z.lazy(() => selectQuerySchema),
    alias: z.string(),
}))

export const sourceSchema: z.ZodType<Source> = z.union([
    directSourceSchema,
    subquerySourceSchema,
])

const baseJoinConditionSchema = z.object({
    leftField: z.string(),
    rightField: z.string(),
})

export const streamJoinWindowSchema = z.object({
    before: z.lazy(() => windowDurationSchema),
    after: z.lazy(() => windowDurationSchema),
    gracePeriod: z.lazy(() => windowDurationSchema).optional(),
})

function baseDataSource<T extends DataSourceType>(sourceType: T) {
    return z.object({
        name: z.string(),
        alias: z.string().optional(),
        sourceType: z.literal(sourceType),
    })
}

export const streamJoinSchema = baseDataSource(DataSourceType.STREAM).merge(z.object({
    window: z.lazy(() => streamJoinWindowSchema),
    conditions: z.array(baseJoinConditionSchema),
}))

export const tableJoinSchema = baseDataSource(DataSourceType.TABLE).merge(z.object({
    conditions: z.array(baseJoinConditionSchema),
}))

export const streamTableJoinSchema = baseDataSource(DataSourceType.TABLE).merge(z.object({
    conditions: z.array(baseJoinConditionSchema),
    foreignKey: z.boolean().optional(),
}))

const timestampConfigSchema = z.object({
    column: z.string(),
    format: z.string().optional(),
    timezone: z.string().optional(),
})

const retentionConfigSchema = z.object({
    size: z.number().optional(),
    time: z.lazy(() => windowDurationSchema).optional(),
})

export const streamSourceOptionsSchema = z.object({
    window: z.lazy(() => windowSpecSchema).optional(),
    partitionBy: z.array(z.string()).optional(),
    timestamp: timestampConfigSchema.optional(),
    format: z.lazy(() => formatConfigSchema).optional(),
    retention: retentionConfigSchema.optional(),
    emitChanges: z.boolean().optional(),
    topicName: z.string().optional(),
    replicas: z.number().optional(),
    partitions: z.number().optional(),
})

export const tableSourceOptionsSchema = z.object({
    clusterBy: z.array(z.string()).optional(),
    timestamp: timestampConfigSchema.optional(),
    format: z.lazy(() => formatConfigSchema).optional(),
    retention: retentionConfigSchema.optional(),
    emitChanges: z.boolean().optional(),
    stateStoreName: z.string().optional(),
    caching: z.boolean().optional(),
    topicName: z.string().optional(),
    replicas: z.number().optional(),
    partitions: z.number().optional(),
})

export const streamStreamJoinClauseSchema = z.object({
    sourceType: z.literal(DataSourceType.STREAM),
    source: sourceSchema.and(
        z.object({
            sourceType: z.literal(DataSourceType.STREAM)
        })
    ),
    sourceOptions: z.lazy(() => streamSourceOptionsSchema).optional(),
    joins: z.array(z.lazy(() => streamJoinSchema)).optional(),
})

export const streamTableJoinClauseSchema = z.object({
    sourceType: z.literal(DataSourceType.STREAM),
    source: sourceSchema.and(
        z.object({
            sourceType: z.literal(DataSourceType.STREAM)
        })
    ),
    sourceOptions: streamSourceOptionsSchema.optional(),
    joins: z.array(streamTableJoinSchema),
})

export const tableTableJoinClauseSchema = z.object({
    sourceType: z.literal(DataSourceType.TABLE),
    source: sourceSchema.and(
        z.object({
            sourceType: z.literal(DataSourceType.TABLE)
        })
    ),
    sourceOptions: tableSourceOptionsSchema.optional(),
    joins: z.array(tableJoinSchema),
})

export const nonJoinClauseSchema = z.object({
    sourceType: z.nativeEnum(DataSourceType),
    source: sourceSchema,
    sourceOptions: z.union([streamSourceOptionsSchema, tableSourceOptionsSchema]).optional(),
})

export const fromClauseSchema: z.ZodType<FromClause> = z.union([
    streamStreamJoinClauseSchema,
    streamTableJoinClauseSchema,
    tableTableJoinClauseSchema,
    nonJoinClauseSchema,
])
