import {
    HavingCondition,
    InHavingAggregateCondition,
    BetweenHavingAggregateCondition,
    OtherHavingAggregateCondition,
    HavingComparisonCondition,
    HavingLogicalCondition,
    GroupBy,
    WindowReference,
    WindowDefinition,
    ComparisonOperator,
    HavingConditionType,
} from '../types';
import { SQLBuilder } from './base';
import { ExpressionBuilder } from './expressionBuilder';
import { PartitionBuilder } from './partitionsBuilder';
import { WindowReferenceBuilder, WindowSpecBuilder } from './windowSpecBuilder';

export class HavingBuilder implements SQLBuilder<HavingCondition> {
    validate (condition: HavingCondition): string | null {
        switch (condition.type) {
            case HavingConditionType.AGGREGATE:
                return this.validateAggregate(condition);
            case HavingConditionType.COMPARISON:
                return this.validateComparison(condition);
            case HavingConditionType.LOGICAL:
                return this.validateLogical(condition);
            default:
                return `Invalid having condition type: ${(condition as any).type}, expected AGGREGATE, COMPARISON, or LOGICAL at ${JSON.stringify(condition)}`;
        }
    }

    build (condition: HavingCondition): string {
        const validation = this.validate(condition);

        if (validation) {
            throw new Error(validation);
        }

        switch (condition.type) {
            case HavingConditionType.AGGREGATE:
                return this.buildAggregate(condition);
            case HavingConditionType.COMPARISON:
                return this.buildComparison(condition);
            case HavingConditionType.LOGICAL:
                return this.buildLogical(condition);
            default:
                throw new Error(`Unknown having condition type at ${JSON.stringify(condition)}`);
        }
    }

    private validateAggregate (condition: InHavingAggregateCondition | BetweenHavingAggregateCondition | OtherHavingAggregateCondition): string | null {
        switch (condition.operator) {
            case ComparisonOperator.IN:
            case ComparisonOperator.NOT_IN:
                return this.validateInAggregate(condition);
            case ComparisonOperator.BETWEEN:
            case ComparisonOperator.NOT_BETWEEN:
                return this.validateBetweenAggregate(condition);
            default:
                return this.validateOtherAggregate(condition);
        }
    }

    private buildAggregate (condition: InHavingAggregateCondition | BetweenHavingAggregateCondition | OtherHavingAggregateCondition): string {
        switch (condition.operator) {
            case ComparisonOperator.IN:
            case ComparisonOperator.NOT_IN:
                return this.buildInAggregate(condition);
            case ComparisonOperator.BETWEEN:
            case ComparisonOperator.NOT_BETWEEN:
                return this.buildBetweenAggregate(condition);
            default:
                return this.buildOtherAggregate(condition);
        }
    }

    private buildInAggregate (condition: InHavingAggregateCondition): string {
        const expressionBuilder = new ExpressionBuilder();
        const func = `${condition.function}(${condition.parameters.map((p) => expressionBuilder.build(p)).join(', ')})`;
        const values = condition.right.map((r) => expressionBuilder.build(r)).join(', ');

        return `${func} ${condition.operator} (${values})`;
    }

    private buildBetweenAggregate (condition: BetweenHavingAggregateCondition): string {
        const expressionBuilder = new ExpressionBuilder();
        const func = `${condition.function}(${condition.parameters.map((p) => expressionBuilder.build(p)).join(', ')})`;
        const start = expressionBuilder.build(condition.right.start);
        const end = expressionBuilder.build(condition.right.end);

        return `${func} ${condition.operator} ${start} AND ${end}`;
    }

    private buildOtherAggregate (condition: OtherHavingAggregateCondition): string {
        const expressionBuilder = new ExpressionBuilder();
        const parameters = condition.parameters.length ? condition.parameters.map((p) => expressionBuilder.build(p)).join(', ') : '*';
        const right = expressionBuilder.build(condition.right);

        return `${condition.function}(${parameters}) ${condition.operator} ${right}`;
    }

    private buildComparison (condition: HavingComparisonCondition): string {
        const left = new ExpressionBuilder().build(condition.left);
        const right = new ExpressionBuilder().build(condition.right);

        return `${left} ${condition.operator} ${right}`;
    }

    private buildLogical (condition: HavingLogicalCondition): string {
        const conditions = condition.conditions.map((c) => this.build(c)).join(` ${condition.operator} `);

        return `(${conditions})`;
    }

    private validateInAggregate (condition: InHavingAggregateCondition): string | null {
        return condition.parameters.every((p) => new ExpressionBuilder().validate(p)) &&
               condition.right.every((r) => new ExpressionBuilder().validate(r))
            ? null
            : `Invalid in condition: ${condition.operator}`;
    }

    private validateBetweenAggregate (condition: BetweenHavingAggregateCondition): string | null {
        return condition.parameters.every((p) => new ExpressionBuilder().validate(p)) &&
               new ExpressionBuilder().validate(condition.right.start) &&
               new ExpressionBuilder().validate(condition.right.end)
            ? null
            : `Invalid between condition: ${condition.operator}`;
    }

    private validateOtherAggregate (condition: OtherHavingAggregateCondition): string | null {
        const validParams = condition.parameters
            .map((p) => new ExpressionBuilder().validate(p));

        const validRight = new ExpressionBuilder().validate(condition.right);
        const valid = [...validParams, validRight].filter((v) => v !== null);

        return valid.length === 0 ? null : `Invalid other condition: ${valid.join(', ')}`;
    }

    private validateComparison (condition: HavingComparisonCondition): string | null {
        const validLeft = new ExpressionBuilder().validate(condition.left);
        const validRight = new ExpressionBuilder().validate(condition.right);

        return validLeft ? validLeft : validRight;
    }

    private validateLogical (condition: HavingLogicalCondition): string | null {
        const valid = condition.conditions
            .map(this.validate.bind(this))
            .filter((x) => x !== null);

        return valid.length === 0 ? null : `Invalid logical condition: ${valid.join(', ')}`;
    }
}

export class GroupByBuilder implements SQLBuilder<GroupBy> {
    validate (groupBy: GroupBy): string | null {
        if (groupBy.columns.length === 0) {
            return `Group by columns are required at ${JSON.stringify(groupBy)}`;
        }

        const results = groupBy.columns.map((col) => new ExpressionBuilder().validate(col.expression)).filter((validation) => validation !== null);

        if (results.length > 0) {
            return `Invalid group by column: ${results.join(', ')}`;
        }

        if ('having' in groupBy && groupBy.having) {
            const havingValidation = new HavingBuilder().validate(groupBy.having);

            if (havingValidation) {
                return havingValidation;
            }
        }

        if ('partitionBy' in groupBy && groupBy.partitionBy) {
            const partitionValidation = new PartitionBuilder().validate(groupBy.partitionBy);

            if (partitionValidation) {
                return partitionValidation;
            }
        }

        if ('window' in groupBy && groupBy.window) {
            if (this.isWindowReference(groupBy.window)) {
                return new WindowReferenceBuilder().validate(groupBy.window);
            }

            if (this.isWindowSpec(groupBy.window)) {
                return new WindowSpecBuilder().validate((groupBy.window as WindowDefinition).spec);
            }
        }

        return null;
    }

    build (groupBy: GroupBy): string {
        const validation = this.validate(groupBy);

        if (validation) {
            throw new Error(validation);
        }

        const columns = groupBy.columns
            .map((col) => {
                const expr = new ExpressionBuilder().build(col.expression);

                return col.alias ? `${expr} AS ${col.alias}` : expr;
            })
            .join(', ');

        let sql = `GROUP BY ${columns}`;

        if ('having' in groupBy && groupBy.having) {
            sql += ` HAVING ${new HavingBuilder().build(groupBy.having)}`;
        }

        if ('partitionBy' in groupBy && groupBy.partitionBy) {
            sql += ` ${new PartitionBuilder().build(groupBy.partitionBy)}`;
        }

        if ('window' in groupBy && groupBy.window) {
            if (this.isWindowReference(groupBy.window)) {
                sql = `WINDOW ${new WindowReferenceBuilder().build(groupBy.window)} ${sql}`;
            }

            if (this.isWindowSpec(groupBy.window)) {
                sql = `WINDOW ${new WindowSpecBuilder().build(groupBy.window.spec)} ${sql}`;
            }
        }

        return sql;
    }

    private isWindowSpec (window: WindowDefinition | WindowReference): window is WindowDefinition {
        return 'spec' in window && window.spec !== undefined;
    }

    private isWindowReference (window: WindowDefinition | WindowReference): window is WindowReference {
        return 'boundaries' in window && window.boundaries !== undefined;
    }
}
