import { z } from 'zod';

import { Expression, ExpressionType, NonAggregateExpression } from '../types';
import { castItemSchema } from './castSchema';
import {
    transformationSchema,
    nonAggregateTransformationSchema,
    aggregateTransformationSchema,
    windowBoundaryTransformationSchema,
    arithmeticTransformationSchema,
    numericTransformationSchema,
} from './transformationSchema';

function baseExpressionSchema<T extends string> (type: T) {
    return z.object({
        type: z.literal(type),
    });
}

export const columnExpressionSchema = baseExpressionSchema(ExpressionType.COLUMN).extend({
    sourceColumn: z.string(),
});

export const literalExpressionSchema = baseExpressionSchema(ExpressionType.LITERAL).extend({
    value: z.union([z.string(), z.number(), z.boolean(), z.null()]),
    dataType: z.optional(z.lazy(() => castItemSchema)),
});

function baseTransformationExpressionSchema<T extends z.ZodType<any>> (type: T) {
    return baseExpressionSchema(ExpressionType.TRANSFORMATION).extend({
        value: type,
    });
}

export const transformationExpressionSchema = baseTransformationExpressionSchema(z.lazy(() => transformationSchema));
export const nonAggregateTransformationExpressionSchema = baseTransformationExpressionSchema(z.lazy(() => nonAggregateTransformationSchema));
export const aggregateExpressionSchema = baseTransformationExpressionSchema(z.lazy(() => aggregateTransformationSchema));
export const windowBoundaryExpressionSchema = baseTransformationExpressionSchema(z.lazy(() => windowBoundaryTransformationSchema));
export const arithmeticExpressionSchema = baseTransformationExpressionSchema(z.lazy(() => arithmeticTransformationSchema));
export const numericExpressionSchema = baseTransformationExpressionSchema(z.lazy(() => numericTransformationSchema));

export const expressionSchema: z.ZodType<Expression> = z.union([
    columnExpressionSchema,
    literalExpressionSchema,
    transformationExpressionSchema,
]);

export const nonAggregateExpressionSchema: z.ZodType<NonAggregateExpression> = z.union([
    columnExpressionSchema,
    literalExpressionSchema,
    nonAggregateTransformationExpressionSchema,
]);
