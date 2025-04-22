import { LogicalOperator, ComparisonOperator, WhereType } from './enums';
import { Expression } from './expressions';

interface BetweenRange {
    start: Expression;
    end: Expression;
}

interface BaseCondition<T extends WhereType> {
    type: T;
}

interface BaseComparisonCondition<E extends ComparisonOperator> extends BaseCondition<WhereType.COMPARISON> {
    operator: E;
    left: Expression;
}

export interface InCondition extends BaseComparisonCondition<ComparisonOperator.IN | ComparisonOperator.NOT_IN> {
    right: Expression[];
}

export interface BetweenCondition extends BaseComparisonCondition<ComparisonOperator.BETWEEN | ComparisonOperator.NOT_BETWEEN> {
    right: BetweenRange;
}

export type NullComparisonCondition = BaseComparisonCondition<ComparisonOperator.IS_NULL | ComparisonOperator.IS_NOT_NULL>;

export interface OtherComparisonCondition extends BaseComparisonCondition<Exclude<ComparisonOperator,
    ComparisonOperator.IN |
    ComparisonOperator.NOT_IN |
    ComparisonOperator.BETWEEN |
    ComparisonOperator.NOT_BETWEEN |
    ComparisonOperator.IS_NULL |
    ComparisonOperator.IS_NOT_NULL
>> {
    right: Expression;
}

export interface LogicalCondition extends BaseCondition<WhereType.LOGICAL> {
    operator: Exclude<LogicalOperator, LogicalOperator.NOT>;
    conditions: WhereCondition[];
}

export interface NotCondition extends BaseCondition<WhereType.LOGICAL> {
    operator: LogicalOperator.NOT;
    condition: WhereCondition;
}

export type WhereCondition = InCondition | BetweenCondition | OtherComparisonCondition | LogicalCondition | NotCondition | NullComparisonCondition;
