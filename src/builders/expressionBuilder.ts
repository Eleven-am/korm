import { ColumnExpression, ExpressionType, Expression, LiteralExpression, NonAggregateExpression } from '../types';
import { SQLBuilder } from './base';
import { TransformationBuilder } from './transformationBuilder';

export class ExpressionBuilder implements SQLBuilder<Expression> {
    validate (expr: Expression | NonAggregateExpression): string | null {
        switch (expr.type) {
            case ExpressionType.LITERAL:
                return this.validateLiteral(expr);
            case ExpressionType.COLUMN:
                return null;
            case ExpressionType.TRANSFORMATION:
                return new TransformationBuilder().validate(expr.value);
            default:
                return `Invalid expression type: ${(expr as any).type}, expected LITERAL, COLUMN, or TRANSFORMATION at ${JSON.stringify(expr)}`;
        }
    }

    build (expr: Expression | NonAggregateExpression): string {
        const validation = this.validate(expr);

        if (validation) {
            throw new Error(validation);
        }

        switch (expr.type) {
            case ExpressionType.LITERAL:
                return this.buildLiteral(expr);
            case ExpressionType.COLUMN:
                return this.buildColumn(expr);
            case ExpressionType.TRANSFORMATION:
                return new TransformationBuilder().build(expr.value);
            default:
                throw new Error(`Unknown expression type: ${(expr as any).type} at ${JSON.stringify(expr)}`);
        }
    }

    private buildLiteral (expr: LiteralExpression): string {
        if (expr.value === null) {
            return 'NULL';
        }
        if (typeof expr.value === 'string') {
            return `'${expr.value}'`;
        }

        return String(expr.value);
    }

    private buildColumn (expr: ColumnExpression): string {
        return expr.sourceColumn;
    }

    private validateLiteral (expr: LiteralExpression): string | null {
        return expr.value === null || ['string', 'number', 'boolean'].includes(typeof expr.value) ? null : `Invalid literal value: ${expr.value} at ${JSON.stringify(expr)}`;
    }
}
