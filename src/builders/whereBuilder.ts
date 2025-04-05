import {
    WhereType,
    ComparisonOperator,
    LogicalOperator,
    Expression,
    WhereCondition,
    InCondition,
    BetweenCondition,
    OtherComparisonCondition,
    LogicalCondition,
    NotCondition,
    NullComparisonCondition,
} from '../types';
import { SQLBuilder } from './base';
import { ExpressionBuilder } from './expressionBuilder';

export class WhereBuilder implements SQLBuilder<WhereCondition> {
    validate (where: WhereCondition): string | null {
        switch (where.type) {
            case WhereType.COMPARISON:
                return this.validateComparison(where);
            case WhereType.LOGICAL:
                switch (where.operator) {
                    case LogicalOperator.AND:
                    case LogicalOperator.OR:
                        return this.validateLogical(where);
                    case LogicalOperator.NOT:
                        return this.validateNot(where);
                    default: return `Invalid logical operator: ${(where as any).operator}`;
                }
            default:
                return `Invalid where condition type: ${(where as any).type}, expected COMPARISON or LOGICAL`;
        }
    }

    build (item: WhereCondition): string {
        const validation = this.validate(item);

        if (validation) {
            throw new Error(validation);
        }

        switch (item.type) {
            case WhereType.COMPARISON:
                return this.buildComparison(item);
            case WhereType.LOGICAL:
                switch (item.operator) {
                    case LogicalOperator.AND:
                    case LogicalOperator.OR:
                        return this.buildLogical(item);
                    case LogicalOperator.NOT:
                        return this.buildNot(item);
                    default:
                        throw new Error('Invalid logical operator');
                }
            default:
                throw new Error(`Unknown where condition type: ${(item as any).type}`);
        }
    }

    buildNullComparison (where: NullComparisonCondition): string {
        return `${new ExpressionBuilder().build(where.left)} ${where.operator}`;
    }

    private validateComparison (where: InCondition | BetweenCondition | OtherComparisonCondition | NullComparisonCondition): string | null {
        switch (where.operator) {
            case ComparisonOperator.IN:
            case ComparisonOperator.NOT_IN:
                return this.validateIn(where);
            case ComparisonOperator.BETWEEN:
            case ComparisonOperator.NOT_BETWEEN:
                return this.validateBetween(where);
            case ComparisonOperator.IS_NULL:
            case ComparisonOperator.IS_NOT_NULL:
                return this.validateExpression(where.left);
            default:
                return this.validateOtherComparison(where);
        }
    }

    private validateIn (where: InCondition): string | null {
        const valid = this.validateExpression(where.left);
        const mapped = where.right.map(this.validateExpression);
        const invalid = [valid, ...mapped].filter((x) => x !== null);

        return invalid.length === 0 ? null : `Invalid in condition: ${invalid.join(', ')}`;
    }

    private validateBetween (where: BetweenCondition): string | null {
        return this.validateExpression(where.left) && this.validateExpression(where.right.start) && this.validateExpression(where.right.end);
    }

    private validateOtherComparison (where: OtherComparisonCondition): string | null {
        return this.validateExpression(where.left) && this.validateExpression(where.right);
    }

    private validateLogical (where: LogicalCondition): string | null {
        const valid = where.conditions
            .map(this.validate.bind(this))
            .filter((x) => x !== null);

        return valid.length === 0 ? null : `Invalid logical condition: ${valid.join(', ')}`;
    }

    private validateNot (where: NotCondition): string | null {
        return this.validate(where.condition);
    }

    private validateExpression (expression: Expression): string | null {
        return new ExpressionBuilder().validate(expression);
    }

    private buildComparison (where: InCondition | BetweenCondition | OtherComparisonCondition | NullComparisonCondition): string {
        switch (where.operator) {
            case ComparisonOperator.IN:
            case ComparisonOperator.NOT_IN:
                return this.buildIn(where);
            case ComparisonOperator.BETWEEN:
            case ComparisonOperator.NOT_BETWEEN:
                return this.buildBetween(where);
            case ComparisonOperator.IS_NULL:
            case ComparisonOperator.IS_NOT_NULL:
                return this.buildNullComparison(where);
            default:
                return this.buildOtherComparison(where);
        }
    }

    private buildLogical (where: LogicalCondition) {
        return `(${where.conditions.map(this.build.bind(this)).join(` ${where.operator} `)})`;
    }

    private buildNot (where: NotCondition) {
        return `NOT (${this.build(where.condition)})`;
    }

    private buildIn (where: InCondition): string {
        return `${new ExpressionBuilder().build(where.left)} ${where.operator} (${where.right.map((x) => new ExpressionBuilder().build(x)).join(', ')})`;
    }

    private buildBetween (where: BetweenCondition): string {
        return `${new ExpressionBuilder().build(where.left)} ${where.operator} ${new ExpressionBuilder().build(where.right.start)} AND ${new ExpressionBuilder().build(where.right.end)}`;
    }

    private buildOtherComparison (where: OtherComparisonCondition): string {
        return `${new ExpressionBuilder().build(where.left)} ${where.operator} ${new ExpressionBuilder().build(where.right)}`;
    }
}
