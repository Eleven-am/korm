import { CastItem } from './cast';
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
    TimeExtractField, WindowFunction, OrderDirection,
} from './enums';
import { Expression } from './expressions';
import { WindowReference, WindowDefinition } from './window';

interface FunctionCall<T extends string> {
    function: T;
    parameters: Expression[];
}

interface TransformationBase<T extends TransformType> {
    type: T;
    alias?: string;
    sourceColumn?: string;
}

interface FunctionTransformation<T extends TransformType, F extends string>
    extends TransformationBase<T>, FunctionCall<F> {}

export type StringTransformation = FunctionTransformation<TransformType.STRING, StringFunction>;

export type NumericTransformation = FunctionTransformation<TransformType.NUMERIC, NumericFunction>;

export type DateTransformation = FunctionTransformation<TransformType.DATE, DateFunction>;

export type CollectionTransformation = FunctionTransformation<TransformType.COLLECTION, CollectionFunction>;

export interface CastTransformation extends TransformationBase<TransformType.CAST> {
    targetType: CastItem;
    sourceExpression: Expression;
}

export interface ArithmeticTransformation extends TransformationBase<TransformType.ARITHMETIC> {
    operator: ArithmeticOperator;
    left: Expression;
    right: Expression;
}

export interface ComparisonTransformation extends TransformationBase<TransformType.COMPARISON> {
    operator: Exclude<ComparisonOperator,
        ComparisonOperator.IN |
        ComparisonOperator.NOT_IN |
        ComparisonOperator.BETWEEN |
        ComparisonOperator.NOT_BETWEEN |
        ComparisonOperator.IS_NULL |
        ComparisonOperator.IS_NOT_NULL
    >;
    left: Expression;
    right: Expression;
}

export interface MultiComparisonTransformation extends TransformationBase<TransformType.COMPARISON> {
    operator: ComparisonOperator.IN | ComparisonOperator.NOT_IN;
    left: Expression;
    right: Expression[];
}

export interface BetweenComparisonTransformation extends TransformationBase<TransformType.COMPARISON> {
    operator: ComparisonOperator.BETWEEN | ComparisonOperator.NOT_BETWEEN;
    left: Expression;
    start: Expression;
    end: Expression;
}

export interface NullComparisonTransformation extends TransformationBase<TransformType.COMPARISON> {
    operator: ComparisonOperator.IS_NULL | ComparisonOperator.IS_NOT_NULL;
    expression: Expression;
}

export interface LogicalTransformation extends TransformationBase<TransformType.LOGICAL> {
    operator: LogicalOperator;
    expressions: Expression[];
}

export interface CaseTransformation extends TransformationBase<TransformType.CASE> {
    conditions: Array<{
        when: Expression;
        then: Expression;
    }>;
    else?: Expression;
}

export interface AggregateTransformation extends FunctionTransformation<TransformType.AGGREGATE, AggregateFunction> {
    window?: WindowReference | WindowDefinition;
}

export interface WindowFunctionTransformation extends FunctionTransformation<TransformType.WINDOW, WindowFunction> {
    over?: {
        alias?: string;
        partitionBy?: Expression[];
        orderBy?: Array<{
            expression: Expression;
            direction?: OrderDirection;
            nulls?: 'FIRST' | 'LAST';
        }>;
    };
}

export interface WindowBoundaryTransformation extends TransformationBase<TransformType.WINDOW_BOUNDARY> {
    boundary: WindowBoundary;
    window?: WindowReference;
}

export interface StructAccessTransformation extends TransformationBase<TransformType.STRUCT_ACCESS> {
    struct: Expression;
    field: string;
}

export interface ArrayAccessTransformation extends TransformationBase<TransformType.ARRAY_ACCESS> {
    array: Expression;
    index: Expression;
}

export interface MapAccessTransformation extends TransformationBase<TransformType.MAP_ACCESS> {
    map: Expression;
    key: Expression;
}

export interface ExtractTransformation extends TransformationBase<TransformType.EXTRACT> {
    field: string | TimeExtractField;
    source: Expression;
}

export type Transformation =
    | StringTransformation
    | NumericTransformation
    | DateTransformation
    | CastTransformation
    | ArithmeticTransformation
    | ComparisonTransformation
    | MultiComparisonTransformation
    | BetweenComparisonTransformation
    | NullComparisonTransformation
    | LogicalTransformation
    | CaseTransformation
    | AggregateTransformation
    | CollectionTransformation
    | WindowFunctionTransformation
    | StructAccessTransformation
    | ArrayAccessTransformation
    | MapAccessTransformation
    | ExtractTransformation
    | WindowBoundaryTransformation;

export type NonAggregateTransformation =
    | StringTransformation
    | NumericTransformation
    | DateTransformation
    | CastTransformation
    | ArithmeticTransformation
    | ComparisonTransformation
    | MultiComparisonTransformation
    | BetweenComparisonTransformation
    | NullComparisonTransformation
    | LogicalTransformation
    | CaseTransformation
    | CollectionTransformation
    | StructAccessTransformation
    | ArrayAccessTransformation
    | MapAccessTransformation
    | ExtractTransformation;
