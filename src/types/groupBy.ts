import { HavingConditionType, AggregateFunction, ComparisonOperator, LogicalOperator, WindowBoundary } from './enums';
import {
    Expression,
    NonAggregateExpression,
    AggregateExpression,
    WindowBoundaryExpression,
    ArithmeticExpression, NumericExpression,
} from './expressions';
import { PartitionBy } from './partition';
import { WindowReference, WindowDefinition } from './window';

export interface HavingConditionBase<T extends HavingConditionType> {
    type: T;
}

export interface BaseHavingAggregateCondition<T extends ComparisonOperator> extends HavingConditionBase<HavingConditionType.AGGREGATE> {
    function: AggregateFunction;
    parameters: Expression[];
    operator: T;
    window?: WindowReference;
}

export interface InHavingAggregateCondition extends BaseHavingAggregateCondition<ComparisonOperator.IN | ComparisonOperator.NOT_IN> {
    right: NonAggregateExpression[];
}

export interface BetweenHavingAggregateCondition extends BaseHavingAggregateCondition<ComparisonOperator.BETWEEN | ComparisonOperator.NOT_BETWEEN> {
    right: {
        start: NonAggregateExpression;
        end: NonAggregateExpression;
    }
}

export interface HavingComparisonCondition extends HavingConditionBase<HavingConditionType.COMPARISON> {
    operator: ComparisonOperator;
    left: ArithmeticExpression | AggregateExpression | WindowBoundaryExpression | NumericExpression;
    right: NonAggregateExpression;
}

export interface OtherHavingAggregateCondition extends BaseHavingAggregateCondition<Exclude<ComparisonOperator,
    ComparisonOperator.IN | ComparisonOperator.NOT_IN |
    ComparisonOperator.BETWEEN | ComparisonOperator.NOT_BETWEEN>> {
    right: NonAggregateExpression;
}

export interface HavingLogicalCondition extends HavingConditionBase<HavingConditionType.LOGICAL> {
    operator: Exclude<LogicalOperator, LogicalOperator.NOT>;
    conditions: HavingCondition[];
}

export type HavingCondition =
    | InHavingAggregateCondition
    | BetweenHavingAggregateCondition
    | OtherHavingAggregateCondition
    | HavingComparisonCondition
    | HavingLogicalCondition;

export interface GroupByColumn {
    expression: NonAggregateExpression;
    alias?: string;
}

export interface BaseGroupBy {
    columns: GroupByColumn[];
}

export interface SimpleGroupBy extends BaseGroupBy {
    having?: never;
    partitionBy?: never;
    window?: never;
}

export interface AggregateGroupBy extends BaseGroupBy {
    having?: HavingCondition;
    partitionBy?: PartitionBy;
}

export interface WindowedGroupBy extends AggregateGroupBy {
    window: WindowReference | WindowDefinition
}

export type GroupBy =
    | SimpleGroupBy
    | AggregateGroupBy
    | WindowedGroupBy;
