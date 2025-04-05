import {
    SelectQuery,
    SimpleSelectQuery,
    AggregateSelectQuery,
    WindowedSelectQuery,
    SimpleOrderBy,
    GroupByOrderBy,
    WindowedOrderBy,
    GroupBy,
    WindowedGroupBy,
    WindowSpec,
    WindowReference,
    OrderDirection,
    SelectType,
    WindowBoundary,
} from '../types';
import { SQLBuilder } from './base';
import { ExpressionBuilder } from './expressionBuilder';
import { FromBuilder } from './fromBuilder';
import { GroupByBuilder } from './groupByBuilder';
import { PartitionBuilder } from './partitionsBuilder';
import { WhereBuilder } from './whereBuilder';
import { WindowSpecBuilder } from './windowSpecBuilder';

export class SelectStatementBuilder implements SQLBuilder<SelectQuery> {
    validate (query: SelectQuery): string | null {
        const fromValidation = new FromBuilder().validate(query.from);

        if (fromValidation) {
            return fromValidation;
        }

        if (query.where) {
            const whereValidation = new WhereBuilder().validate(query.where);

            if (whereValidation) {
                return whereValidation;
            }
        }

        switch (query.type) {
            case SelectType.STAR:
                return null;
            case SelectType.COLUMN:
                return this.validateColumnSelect(query);
            default:
                return `Invalid select type: ${(query as any).type}, expected STAR or COLUMN at ${JSON.stringify(query)}`;
        }
    }

    build (query: SelectQuery): string {
        const validation = this.validate(query);

        if (validation) {
            throw new Error(validation);
        }

        let sql = this.buildSelectClause(query);

        sql += ` ${new FromBuilder().build(query.from)}`;

        if (query.where) {
            sql += ` WHERE ${new WhereBuilder().build(query.where)}`;
        }

        if (this.hasPartitionBy(query) && query.partitionBy) {
            sql += ` ${new PartitionBuilder().build(query.partitionBy)}`;
        }

        if (this.isColumnSelectQuery(query) && this.hasGroupBy(query)) {
            sql += ` ${new GroupByBuilder().build(query.groupBy)}`;
        }

        if (this.hasOrderBy(query) && query.orderBy) {
            sql += ` ${this.buildOrderBy(query.orderBy)}`;
        }

        if (query.limit !== undefined) {
            sql += ` LIMIT ${query.limit}`;
        }

        if (query.emit) {
            sql += ` EMIT ${query.emit}`;
        }

        return `${sql};`;
    }

    private validateColumnSelect (query: SimpleSelectQuery | AggregateSelectQuery | WindowedSelectQuery): string | null {
        const columnValidation = query.columns
            .map((col) => new ExpressionBuilder().validate(col.expression))
            .filter((validation) => validation !== null);

        if (columnValidation.length > 0) {
            return `Invalid select column: ${columnValidation.join(', ')}`;
        }

        if (this.hasGroupBy(query)) {
            const groupByValidation = this.validateGroupByQuery(query);

            if (groupByValidation) {
                return groupByValidation;
            }

            if (this.hasOrderBy(query) && query.orderBy) {
                const orderByValidation = this.validateOrderBy(query.orderBy);

                if (orderByValidation) {
                    return orderByValidation;
                }
            }
        }

        if (this.hasPartitionBy(query) && query.partitionBy) {
            const partitionByValidation = new PartitionBuilder().validate(query.partitionBy);

            if (partitionByValidation) {
                return partitionByValidation;
            }
        }

        return null;
    }

    private validateGroupByQuery (query: AggregateSelectQuery | WindowedSelectQuery): string | null {
        if (this.isWindowedGroupBy(query.groupBy)) {
            const windowValidation = this.validateWindowedGroupBy(query.groupBy);

            if (windowValidation) {
                return windowValidation;
            }
        }

        return new GroupByBuilder().validate(query.groupBy);
    }

    private validateWindowedGroupBy (groupBy: WindowedGroupBy): string | null {
        if (!this.isWindowSpec(groupBy.window) && !this.isWindowReference(groupBy.window)) {
            return `Invalid window definition, expected WindowReference or WindowSpec at ${JSON.stringify(groupBy.window)}`;
        }

        if (this.isWindowSpec(groupBy.window)) {
            const windowSpecValidation = new WindowSpecBuilder().validate(groupBy.window.spec);

            if (windowSpecValidation) {
                return windowSpecValidation;
            }
        }

        if (groupBy.window.boundaries) {
            return groupBy.window.boundaries.every((boundary) => Object.values(WindowBoundary).includes(boundary))
                ? null
                : `Invalid window boundaries at ${JSON.stringify(groupBy.window.boundaries)}`;
        }

        return null;
    }

    private validateOrderBy (orderBy: SimpleOrderBy | GroupByOrderBy | WindowedOrderBy): string | null {
        const columnValidation = orderBy.columns
            .map((col) => new ExpressionBuilder().validate(col.expression))
            .filter((validation) => validation !== null);

        if (columnValidation.length > 0) {
            return `Invalid order by column: ${columnValidation.join(', ')}`;
        }

        const directionInValidation = orderBy.columns
            .map((col) => col.direction)
            .filter((direction) => direction && !Object.values(OrderDirection).includes(direction));

        if (directionInValidation.length >= 1) {
            return `Invalid order by direction: ${directionInValidation.join(', ')}`;
        }

        return null;
    }

    private isWindowSpec (window: WindowReference | { spec: WindowSpec, boundaries?: WindowBoundary[] }): window is { spec: WindowSpec, boundaries?: WindowBoundary[] } {
        return 'spec' in window && window.spec !== undefined;
    }

    private isWindowReference (window: WindowReference | { spec: WindowSpec, boundaries?: WindowBoundary[] }): window is WindowReference {
        return 'name' in window && window.name !== undefined;
    }

    private isColumnSelectQuery (query: SelectQuery): query is SimpleSelectQuery | AggregateSelectQuery | WindowedSelectQuery {
        return query.type === SelectType.COLUMN;
    }

    private hasGroupBy (query: SelectQuery): query is AggregateSelectQuery | WindowedSelectQuery {
        return 'groupBy' in query && query.groupBy !== undefined;
    }

    private hasOrderBy (query: SelectQuery): query is SimpleSelectQuery | AggregateSelectQuery | WindowedSelectQuery {
        return 'orderBy' in query && query.orderBy !== undefined;
    }

    private hasPartitionBy (query: SelectQuery): query is SimpleSelectQuery | AggregateSelectQuery | WindowedSelectQuery {
        return 'partitionBy' in query && query.partitionBy !== undefined;
    }

    private isWindowedGroupBy (groupBy: GroupBy): groupBy is WindowedGroupBy {
        return 'window' in groupBy && groupBy.window !== undefined;
    }

    private buildSelectClause (query: SelectQuery): string {
        if (query.type === SelectType.STAR) {
            return 'SELECT *';
        }

        const columnSql = query.columns.map((col) => {
            const expr = new ExpressionBuilder().build(col.expression);

            return col.alias ? `${expr} AS ${col.alias}` : expr;
        }).join(', ');

        return `SELECT ${columnSql}`;
    }

    private buildOrderBy (orderBy: SimpleOrderBy | GroupByOrderBy | WindowedOrderBy): string {
        const orderClauses = orderBy.columns.map((col) => {
            let orderSql = new ExpressionBuilder().build(col.expression);

            if (col.direction) {
                orderSql += ` ${col.direction}`;
            }
            if (col.nulls) {
                orderSql += ` ${col.nulls}`;
            }

            return orderSql;
        });

        return `ORDER BY ${orderClauses.join(', ')}`;
    }
}
