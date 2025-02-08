import { SelectType, OrderDirection, NullHandling, OrderByType, EmitType } from './enums';
import { NonAggregateExpression, AggregateExpression, WindowBoundaryExpression } from './expressions';
import { FromClause } from './from';
import { GroupBy, WindowedGroupBy } from './groupBy';
import { PartitionBy } from './partition';
import { WhereCondition } from './where';

interface BaseSelectColumn {
    type: SelectType.COLUMN;
    alias?: string;
}

type SimpleOrderExpression = NonAggregateExpression;
type GroupByOrderExpression = NonAggregateExpression | AggregateExpression;
type WindowedOrderExpression = NonAggregateExpression | AggregateExpression | WindowBoundaryExpression;

interface BaseOrderByColumn<T extends SimpleOrderExpression | GroupByOrderExpression | WindowedOrderExpression, D extends OrderByType> {
    expression: T;
    direction?: OrderDirection;
    nulls?: NullHandling;
    type: D;
}

export interface SimpleOrderBy {
    columns: BaseOrderByColumn<SimpleOrderExpression, OrderByType.SIMPLE>[];
}

export interface GroupByOrderBy {
    columns: BaseOrderByColumn<GroupByOrderExpression, OrderByType.GROUP_BY>[];
}

export interface WindowedOrderBy {
    columns: BaseOrderByColumn<WindowedOrderExpression, OrderByType.WINDOWED>[];
}

interface BaseSelectQuery<T extends SelectType> {
    type: T;
    from: FromClause;
    where?: WhereCondition;
    limit?: number;
    emit: EmitType;
}

interface SimpleSelectColumn extends BaseSelectColumn {
    expression: NonAggregateExpression;
}

interface AggregateSelectColumn extends BaseSelectColumn {
    expression: AggregateExpression;
}

interface NonAggregateSelectColumn extends BaseSelectColumn {
    expression: NonAggregateExpression;
}

interface WindowedSelectColumn extends BaseSelectColumn {
    expression: WindowedOrderExpression;
}

export interface SimpleSelectQuery extends BaseSelectQuery<SelectType.COLUMN> {
    columns: SimpleSelectColumn[];
    groupBy?: never;
    orderBy?: SimpleOrderBy;
    partitionBy?: never;
}

export interface AggregateSelectQuery extends BaseSelectQuery<SelectType.COLUMN> {
    columns: (AggregateSelectColumn | NonAggregateSelectColumn)[];
    groupBy: GroupBy;
    orderBy?: GroupByOrderBy;
    partitionBy?: PartitionBy;
}

export interface SelectStar extends BaseSelectQuery<SelectType.STAR> {}

export interface WindowedSelectQuery extends Omit<AggregateSelectQuery, 'orderBy' | 'columns'> {
    columns: WindowedSelectColumn[];
    groupBy: WindowedGroupBy;
    orderBy?: WindowedOrderBy;
}

export type SelectQuery =
    | SimpleSelectQuery
    | AggregateSelectQuery
    | WindowedSelectQuery
    | SelectStar;
