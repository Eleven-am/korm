import { z, EnumLike } from 'zod';
import { identifierSchema } from './baseSchema';
import { castItemSchema } from './castSchema';
import { expressionSchema } from './expressionSchema';
import {
    TransformType,
    StringFunction,
    NumericFunction,
    DateFunction,
    ArithmeticOperator,
    ComparisonOperator,
    LogicalOperator,
    CollectionFunction,
    AggregateFunction,
    WindowBoundary,
    TimeExtractField,
    WindowFunction,
    OrderDirection,
    Transformation, NonAggregateTransformation,
} from '../types';
import { windowReferenceSchema, windowDefinitionSchema } from './windowSchema';

function functionCall<T extends EnumLike>(functionName: T) {
    return z.object({
        function: z.nativeEnum(functionName),
        parameters: z.array(z.lazy(() => expressionSchema)),
    })
}

function transformationBase<T extends TransformType>(type: T) {
    return z.object({
        type: z.literal(type),
        alias: identifierSchema.optional(),
        sourceColumn: identifierSchema.optional(),
    })
}

function functionTransformation<T extends TransformType, F extends EnumLike> (type: T, functionName: F) {
    return functionCall(functionName).merge(transformationBase(type));
}

export const stringTransformationSchema = functionTransformation(TransformType.STRING, StringFunction);
export const numericTransformationSchema = functionTransformation(TransformType.NUMERIC, NumericFunction);
export const dateTransformationSchema = functionTransformation(TransformType.DATE, DateFunction);
export const collectionTransformationSchema = functionTransformation(TransformType.COLLECTION, CollectionFunction);

export const castTransformationSchema = transformationBase(TransformType.CAST).merge(z.object({
    targetType: castItemSchema,
    sourceExpression: z.lazy(() => expressionSchema),
}))

export const arithmeticTransformationSchema = transformationBase(TransformType.ARITHMETIC).merge(z.object({
    operator: z.nativeEnum(ArithmeticOperator),
    left: z.lazy(() => expressionSchema),
    right: z.lazy(() => expressionSchema),
}))

export const baseComparisonTransformationSchema = transformationBase(TransformType.COMPARISON).merge(z.object({
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
    left: z.lazy(() => expressionSchema),
    right: z.lazy(() => expressionSchema),
}))

export const multiComparisonTransformationSchema = transformationBase(TransformType.COMPARISON).merge(z.object({
    operator: z.enum([
        ComparisonOperator.IN,
        ComparisonOperator.NOT_IN,
    ]),
    left: z.lazy(() => expressionSchema),
    right: z.array(z.lazy(() => expressionSchema)),
}))

export const betweenComparisonTransformationSchema = transformationBase(TransformType.COMPARISON).merge(z.object({
    operator: z.enum([
        ComparisonOperator.BETWEEN,
        ComparisonOperator.NOT_BETWEEN,
    ]),
    left: z.lazy(() => expressionSchema),
    start: z.lazy(() => expressionSchema),
    end: z.lazy(() => expressionSchema),
}))

export const nullComparisonTransformationSchema = transformationBase(TransformType.COMPARISON).merge(z.object({
    operator: z.enum([
        ComparisonOperator.IS_NULL,
        ComparisonOperator.IS_NOT_NULL,
    ]),
    expression: z.lazy(() => expressionSchema),
}))

export const comparisonTransformationSchema = z.union([
    baseComparisonTransformationSchema,
    multiComparisonTransformationSchema,
    betweenComparisonTransformationSchema,
    nullComparisonTransformationSchema,
])

export const logicalTransformationSchema = transformationBase(TransformType.LOGICAL).merge(z.object({
    operator: z.nativeEnum(LogicalOperator),
    expressions: z.array(z.lazy(() => expressionSchema)),
}))

export const caseTransformationSchema = transformationBase(TransformType.CASE).merge(z.object({
    conditions: z.array(z.object({
        when: z.lazy(() => expressionSchema),
        then: z.lazy(() => expressionSchema),
    })),
    else: z.lazy(() => expressionSchema).optional(),
}))

export const aggregateTransformationSchema = functionCall(AggregateFunction).merge(transformationBase(TransformType.AGGREGATE)).merge(z.object({
    window: z.union([windowReferenceSchema, windowDefinitionSchema]).optional(),
}))

export const windowFunctionTransformationSchema = functionCall(WindowFunction).merge(transformationBase(TransformType.WINDOW)).merge(z.object({
    over: z.object({
        alias: identifierSchema.optional(),
        partitionBy: z.array(z.lazy(() => expressionSchema)).optional(),
        orderBy: z.array(z.object({
            expression: z.lazy(() => expressionSchema),
            direction: z.nativeEnum(OrderDirection).optional(),
            nulls: z.enum(['FIRST', 'LAST']).optional(),
        })).optional(),
    }).optional(),
}))

export const windowBoundaryTransformationSchema = transformationBase(TransformType.WINDOW_BOUNDARY).merge(z.object({
    boundary: z.nativeEnum(WindowBoundary),
    window: windowReferenceSchema.optional(),
}))

export const structAccessTransformationSchema = transformationBase(TransformType.STRUCT_ACCESS).merge(z.object({
    struct: z.lazy(() => expressionSchema),
    field: identifierSchema,
}))

export const arrayAccessTransformationSchema = transformationBase(TransformType.ARRAY_ACCESS).merge(z.object({
    array: z.lazy(() => expressionSchema),
    index: z.lazy(() => expressionSchema),
}))

export const mapAccessTransformationSchema = transformationBase(TransformType.MAP_ACCESS).merge(z.object({
    map: z.lazy(() => expressionSchema),
    key: z.lazy(() => expressionSchema),
}))

export const extractTransformationSchema = transformationBase(TransformType.EXTRACT).merge(z.object({
    field: z.union([z.nativeEnum(TimeExtractField), identifierSchema]),
    source: z.lazy(() => expressionSchema),
}))

export const transformationSchema: z.ZodSchema<Transformation> = z.union([
    stringTransformationSchema,
    numericTransformationSchema,
    dateTransformationSchema,
    castTransformationSchema,
    arithmeticTransformationSchema,
    comparisonTransformationSchema,
    logicalTransformationSchema,
    caseTransformationSchema,
    aggregateTransformationSchema,
    collectionTransformationSchema,
    windowFunctionTransformationSchema,
    structAccessTransformationSchema,
    arrayAccessTransformationSchema,
    mapAccessTransformationSchema,
    extractTransformationSchema,
    windowBoundaryTransformationSchema,
])

export const nonAggregateTransformationSchema: z.ZodSchema<NonAggregateTransformation> = z.union([
    stringTransformationSchema,
    numericTransformationSchema,
    dateTransformationSchema,
    castTransformationSchema,
    arithmeticTransformationSchema,
    logicalTransformationSchema,
    caseTransformationSchema,
    collectionTransformationSchema,
    structAccessTransformationSchema,
    arrayAccessTransformationSchema,
    mapAccessTransformationSchema,
    extractTransformationSchema,
    comparisonTransformationSchema,
])
