import { z } from 'zod';
import { SelectQuery, SelectType, OrderDirection, NullHandling, OrderByType, EmitType } from '../types';
import {
    nonAggregateExpressionSchema,
    aggregateExpressionSchema,
    windowBoundaryExpressionSchema,
} from './expressionSchema';
import { fromClauseSchema } from './fromSchema';
import { groupBySchema, windowedGroupBySchema } from './groupBySchema';
import { partitionBySchema } from './partitionSchema';
import { whereConditionSchema } from './whereSchema';

const simpleOrderExpressionSchema = z.lazy(() => nonAggregateExpressionSchema);
const groupByOrderExpressionSchema = z.union([z.lazy(() => nonAggregateExpressionSchema), z.lazy(() => aggregateExpressionSchema)]);
const windowedOrderExpressionSchema = z.union([
    nonAggregateExpressionSchema,
    aggregateExpressionSchema,
    windowBoundaryExpressionSchema
]);

function baseOrderByColumnSchema<T extends z.ZodType, D extends OrderByType> (expression: T, type: D) {
    return z.object({
        columns: z.array(z.object({
            expression,
            direction: z.nativeEnum(OrderDirection).optional(),
            nulls: z.nativeEnum(NullHandling).optional(),
            type: z.literal(type),
       }))
    });
}

function baseSelectQuerySchema<T extends SelectType> (type: T) {
    return z.object({
        type: z.literal(type),
        from: z.lazy(() => fromClauseSchema),
        where: z.lazy(() => whereConditionSchema).optional(),
        limit: z.number()
            .optional(),
        emit: z.nativeEnum(EmitType),
    });
}

function baseSelectColumnSchema <T extends z.ZodType>(expression: T) {
    return z.object({
        type: z.literal(SelectType.COLUMN),
        alias: z.string().optional(),
        expression: expression,
    });
}

export const simpleOrderBySchema = baseOrderByColumnSchema(simpleOrderExpressionSchema, OrderByType.SIMPLE);
export const groupByOrderBySchema = baseOrderByColumnSchema(groupByOrderExpressionSchema, OrderByType.GROUP_BY);
export const windowedOrderBySchema = baseOrderByColumnSchema(windowedOrderExpressionSchema, OrderByType.WINDOWED);

export const selectStarSchema = baseSelectQuerySchema(SelectType.STAR);
export const simpleSelectColumnSchema = baseSelectColumnSchema(z.lazy(() => nonAggregateExpressionSchema));
export const aggregateSelectColumnSchema = baseSelectColumnSchema(z.lazy(() => aggregateExpressionSchema));
export const nonAggregateSelectColumnSchema = baseSelectColumnSchema(z.lazy(() => nonAggregateExpressionSchema));
export const windowedSelectColumnSchema = baseSelectColumnSchema(windowedOrderExpressionSchema);

export const simpleSelectQuerySchema = baseSelectQuerySchema(SelectType.COLUMN).merge(z.object({
    columns: z.array(simpleSelectColumnSchema),
    orderBy: simpleOrderBySchema,
}));

export const aggregateSelectQuerySchema = baseSelectQuerySchema(SelectType.COLUMN).merge(z.object({
    columns: z.array(z.union([aggregateSelectColumnSchema, nonAggregateSelectColumnSchema])),
    groupBy: groupBySchema,
    orderBy: groupByOrderBySchema.optional(),
    partitionBy: partitionBySchema.optional(),
}));

export const windowedSelectQuerySchema = z.object({
    ...aggregateSelectQuerySchema.shape,
    columns: z.array(z.lazy(() => windowedSelectColumnSchema)),
    orderBy: z.lazy(() => windowedOrderBySchema).optional(),
    groupBy: z.lazy(() => windowedGroupBySchema),
});

export const selectQuerySchema: z.ZodType<SelectQuery> = z.union([
    simpleSelectQuerySchema,
    aggregateSelectQuerySchema,
    windowedSelectQuerySchema,
    selectStarSchema,
]);
