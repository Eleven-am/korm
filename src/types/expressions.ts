import { CastItem } from './cast';
import { ExpressionType } from './enums';
import {
    Transformation,
    NonAggregateTransformation,
    AggregateTransformation,
    WindowBoundaryTransformation, ArithmeticTransformation, NumericTransformation,
} from './transformation';

interface BaseExpression<T extends ExpressionType> {
    type: T;
}

export interface ColumnExpression extends BaseExpression<ExpressionType.COLUMN> {
    sourceColumn: string;
}

export interface LiteralExpression extends BaseExpression<ExpressionType.LITERAL> {
    value: string | number | boolean | null;
    dataType?: CastItem;
}

export interface TransformationExpression extends BaseExpression<ExpressionType.TRANSFORMATION> {
    value: Transformation;
}

interface NonAggregateTransformationExpression extends BaseExpression<ExpressionType.TRANSFORMATION> {
    value: NonAggregateTransformation;
}

export interface AggregateExpression extends BaseExpression<ExpressionType.TRANSFORMATION> {
    value: AggregateTransformation;
}

export interface WindowBoundaryExpression extends BaseExpression<ExpressionType.TRANSFORMATION> {
    value: WindowBoundaryTransformation;
}

export interface ArithmeticExpression extends BaseExpression<ExpressionType.TRANSFORMATION> {
    value: ArithmeticTransformation;
}

export interface NumericExpression extends BaseExpression<ExpressionType.TRANSFORMATION> {
    value: NumericTransformation;
}

export type Expression =
    | ColumnExpression
    | LiteralExpression
    | TransformationExpression;

export type NonAggregateExpression =
    | ColumnExpression
    | LiteralExpression
    | NonAggregateTransformationExpression;

