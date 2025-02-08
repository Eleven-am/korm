import { z } from 'zod';
import {
    HavingConditionType,
    AggregateFunction,
    ComparisonOperator,
    LogicalOperator,
    HavingCondition,
    GroupBy,
} from '../types';
import {
    expressionSchema,
    nonAggregateExpressionSchema,
    aggregateExpressionSchema,
    windowBoundaryExpressionSchema,
    arithmeticExpressionSchema,
    numericExpressionSchema,
} from './expressionSchema';
import { partitionBySchema } from './partitionSchema';
import { windowReferenceSchema, windowDefinitionSchema } from './windowSchema';

function havingConditionBase<T extends HavingConditionType>(type: T) {
    return z.object({
        type: z.literal(type),
    });
}

const baseHavingAggregateCondition = havingConditionBase(HavingConditionType.AGGREGATE).merge(z.object({
    function: z.nativeEnum(AggregateFunction),
    parameters: z.array(z.lazy(() => expressionSchema)),
    window: z.lazy(() => windowReferenceSchema).optional(),
}));

export const inNotInHavingAggregateConditionSchema = baseHavingAggregateCondition.merge(z.object({
    right: z.array(z.lazy(() => nonAggregateExpressionSchema)),
    operator: z.enum([ComparisonOperator.IN, ComparisonOperator.NOT_IN]),
}));

export const betweenNotBetweenHavingAggregateConditionSchema = baseHavingAggregateCondition.merge(z.object({
    right: z.object({
        start: z.lazy(() => nonAggregateExpressionSchema),
        end: z.lazy(() => nonAggregateExpressionSchema),
    }),
    operator: z.enum([ComparisonOperator.BETWEEN, ComparisonOperator.NOT_BETWEEN]),
}));

export const havingComparisonConditionSchema = havingConditionBase(HavingConditionType.COMPARISON).merge(z.object({
    operator: z.nativeEnum(ComparisonOperator),
    left: z.union([
        z.lazy(() => arithmeticExpressionSchema),
        z.lazy(() => aggregateExpressionSchema),
        z.lazy(() => windowBoundaryExpressionSchema),
        z.lazy(() => numericExpressionSchema),
    ]),
    right: z.lazy(() => nonAggregateExpressionSchema),
}));

export const otherHavingAggregateConditionSchema = baseHavingAggregateCondition.merge(z.object({
    right: z.lazy(() => nonAggregateExpressionSchema),
    operator: z.enum([
        ComparisonOperator.EQUAL,
        ComparisonOperator.NOT_EQUAL,
        ComparisonOperator.GREATER_THAN,
        ComparisonOperator.GREATER_THAN_OR_EQUAL,
        ComparisonOperator.LESS_THAN,
        ComparisonOperator.LESS_THAN_OR_EQUAL,
        ComparisonOperator.LIKE,
        ComparisonOperator.NOT_LIKE,
    ]),
}));

export const havingLogicalConditionSchema = havingConditionBase(HavingConditionType.LOGICAL).merge(z.object({
    operator: z.enum([LogicalOperator.AND, LogicalOperator.OR]),
    conditions: z.array(z.lazy(() => havingConditionSchema)),
}));

export const aggregateHavingConditionSchema = z.discriminatedUnion('operator', [
    otherHavingAggregateConditionSchema,
    inNotInHavingAggregateConditionSchema,
    betweenNotBetweenHavingAggregateConditionSchema,
]);

export const havingConditionSchema: z.ZodType<HavingCondition> = z.union([
    havingLogicalConditionSchema,
    aggregateHavingConditionSchema,
    havingComparisonConditionSchema,
]);

export const groupByColumnSchema = z.object({
    expression: z.lazy(() => nonAggregateExpressionSchema),
    alias: z.string().optional(),
});

const baseGroupBySchema = z.object({
    columns: z.array(z.lazy(() => groupByColumnSchema)),
});

export const simpleGroupBySchema = baseGroupBySchema;

export const aggregateGroupBySchema = baseGroupBySchema.merge(z.object({
    having: z.lazy(() => havingConditionSchema).optional(),
    partimergetionBy: z.lazy(() => partitionBySchema).optional(),
}));

export const windowedGroupBySchema = aggregateGroupBySchema.merge(z.object({
    window: z.union([
        z.lazy(() => windowReferenceSchema),
        z.lazy(() => windowDefinitionSchema),
    ]),
}));

export const groupBySchema: z.ZodType<GroupBy> = z.union([
    simpleGroupBySchema,
    aggregateGroupBySchema,
    windowedGroupBySchema,
]);
