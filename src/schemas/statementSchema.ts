import { z } from 'zod';
import { castItemSchema } from './castSchema';
import { formatConfigSchema } from './baseSchema';
import { selectQuerySchema } from './selectSchema';
import { literalExpressionSchema } from './expressionSchema';
import {
    InsertType,
    QueryType,
    DataSourceType,
    PropertyAction,
    CreateType,
    CreateStatement,
    KSQLStatement, ShowType,
} from '../types';

const insertTargetSchema = z.object({
    name: z.string(),
    columns: z.array(z.string()).optional(),
})

const insertValueSchema = z.object({
    column: z.string(),
    value: z.lazy(() => literalExpressionSchema),
})

function insertBaseQuery<T extends z.ZodType<any>, U extends InsertType>(dataSchema: T, type: U) {
    return z.object({
        target: insertTargetSchema,
        data: dataSchema,
        type: z.literal(type),
        schema: z.object({
            fields: z.array(z.object({
                name: z.string(),
                type: z.lazy(() => castItemSchema),
                required: z.boolean().optional(),
            })),
        }).optional(),
    })
}

export const insertValueQuerySchema = insertBaseQuery(z.array(insertValueSchema), InsertType.VALUES);
export const insertSelectQuerySchema = insertBaseQuery(z.lazy(() => selectQuerySchema), InsertType.SELECT);

export const insertQuerySchema = z.object({
    type: z.literal(QueryType.INSERT),
    statement: z.union([z.lazy(() => insertValueQuerySchema), z.lazy(() => insertSelectQuerySchema)]),
})

export const dropStatementSchema = z.object({
    type: z.literal(QueryType.DROP),
    sourceType: z.nativeEnum(DataSourceType),
    sourceName: z.string(),
    options: z.object({
        ifExists: z.boolean().optional(),
        deleteTopic: z.boolean().optional(),
    }).optional(),
})

export const terminateQuerySchema = z.object({
    type: z.literal(QueryType.TERMINATE),
    queryId: z.string(),
})

export const propertyStatementSchema = z.object({
    type: z.literal(QueryType.PROPERTY),
    action: z.nativeEnum(PropertyAction),
    property: z.string(),
    value: z.string().optional(),
})

const createSourceOptionsSchema = z.object({
    format: z.lazy(() => formatConfigSchema),
    kafkaTopic: z.string().optional(),
    partitions: z.number().optional(),
    replicas: z.number().optional(),
    timestampColumn: z.object({
        name: z.string(),
        format: z.string().optional(),
    }).optional(),
})

const tableOptionsSchema = createSourceOptionsSchema.merge(z.object({
    stateStoreName: z.string().optional(),
    caching: z.boolean().optional(),
}))

const schemaFieldSchema = z.object({
    name: z.string(),
    type: z.lazy(() => castItemSchema),
    key: z.boolean().optional(),
})

function createSourceStatementSchema<T extends DataSourceType>(sourceType: T) {
    return z.object({
        type: z.literal(QueryType.CREATE),
        createType: z.literal(CreateType.SOURCE),
        sourceType: z.literal(sourceType),
        sourceName: z.string(),
        ifNotExists: z.boolean().optional(),
        schema: z.array(schemaFieldSchema),
        options: z.union([
            createSourceOptionsSchema,
            tableOptionsSchema,
        ]),
    })
}

export const createSourceStatementStreamSchema = createSourceStatementSchema(DataSourceType.STREAM);
export const createSourceStatementTableSchema = createSourceStatementSchema(DataSourceType.TABLE);

function createAsSelectStatementSchema<T extends DataSourceType>(sourceType: T) {
    return z.object({
        type: z.literal(QueryType.CREATE),
        createType: z.literal(CreateType.AS_SELECT),
        sourceType: z.literal(sourceType),
        sourceName: z.string(),
        ifNotExists: z.boolean().optional(),
        select: z.lazy(() => selectQuerySchema),
        options: z.union([
            createSourceOptionsSchema,
            tableOptionsSchema,
        ]),
    })
}

export const createAsSelectStatementStreamSchema = createAsSelectStatementSchema(DataSourceType.STREAM);
export const createAsSelectStatementTableSchema = createAsSelectStatementSchema(DataSourceType.TABLE);

export const createStatementSchema: z.ZodType<CreateStatement> = z.union([
    createSourceStatementStreamSchema,
    createSourceStatementTableSchema,
    createAsSelectStatementStreamSchema,
    createAsSelectStatementTableSchema,
])

export const listStatementSchema = z.object({
    type: z.literal(QueryType.LIST),
    sourceType: z.nativeEnum(DataSourceType),
    extended: z.boolean().optional(),
})

export const showStatementSchema = z.object({
    type: z.literal(QueryType.SHOW),
    showType: z.nativeEnum(ShowType),
    extended: z.boolean().optional(),
})

export const explainQuerySchema = z.object({
    type: z.literal(QueryType.EXPLAIN),
    statement: z.union([selectQuerySchema, createAsSelectStatementStreamSchema]),
    analyze: z.boolean().optional(),
})

export const describeQuerySchema = z.object({
    type: z.literal(QueryType.DESCRIBE),
    target: z.object({
        type: z.nativeEnum(DataSourceType),
        name: z.string(),
    }),
    extended: z.boolean().optional(),
})

export const ksqlStatementSchema: z.ZodType<KSQLStatement> = z.union([
    selectQuerySchema,
    insertQuerySchema,
    dropStatementSchema,
    createStatementSchema,
    terminateQuerySchema,
    propertyStatementSchema,
    listStatementSchema,
    showStatementSchema,
    explainQuerySchema,
    describeQuerySchema,
])

