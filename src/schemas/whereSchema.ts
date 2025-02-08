import { z } from 'zod';
import { LogicalOperator, ComparisonOperator, WhereType, WhereCondition } from '../types';
import { expressionSchema } from './expressionSchema';

const betweenRangeSchema = z.object({
    start: expressionSchema,
    end: expressionSchema,
})

function baseCondition<T extends WhereType>(type: T) {
    return z.object({
        type: z.literal(type),
    })
}

function baseComparisonCondition<T extends ComparisonOperator>(operator: T) {
    return baseCondition(WhereType.COMPARISON).merge(z.object({
        operator: z.literal(operator),
        left: z.lazy(() => expressionSchema),
    }))
}

export const inConditionSchema = baseComparisonCondition(ComparisonOperator.IN).merge(z.object({
    right: z.array(z.lazy(() => expressionSchema)),
}))

export const notInConditionSchema = baseComparisonCondition(ComparisonOperator.NOT_IN).merge(z.object({
    right: z.array(z.lazy(() => expressionSchema)),
}))

export const betweenConditionSchema = baseComparisonCondition(ComparisonOperator.BETWEEN).merge(z.object({
    right: betweenRangeSchema,
}))

export const notBetweenConditionSchema = baseComparisonCondition(ComparisonOperator.NOT_BETWEEN).merge(z.object({
    right: betweenRangeSchema,
}))

export const nullComparisonSchema = baseCondition(WhereType.COMPARISON).merge(z.object({
    operator: z.enum([ComparisonOperator.IS_NULL, ComparisonOperator.IS_NOT_NULL]),
    left: z.lazy(() => expressionSchema),
}));

export const otherComparisonConditionSchema = baseCondition(WhereType.COMPARISON).merge(z.object({
    right: z.lazy(() => expressionSchema),
    left: z.lazy(() => expressionSchema),
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
}))

export const logicalConditionSchema = baseCondition(WhereType.LOGICAL).merge(z.object({
    operator: z.enum([LogicalOperator.AND, LogicalOperator.OR]),
    conditions: z.array(z.lazy(() => whereConditionSchema)),
}))

export const notConditionSchema = baseCondition(WhereType.LOGICAL).merge(z.object({
    operator: z.literal(LogicalOperator.NOT),
    condition: z.lazy(() => whereConditionSchema),
}))

export const whereConditionSchema: z.ZodSchema<WhereCondition> = z.discriminatedUnion('operator', [
    inConditionSchema,
    notInConditionSchema,
    betweenConditionSchema,
    notBetweenConditionSchema,
    otherComparisonConditionSchema,
    logicalConditionSchema,
    notConditionSchema,
    nullComparisonSchema,
])
