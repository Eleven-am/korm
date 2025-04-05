import {
    Expression,
    Transformation,
    TransformType,
    StringTransformation,
    NumericTransformation,
    DateTransformation,
    CastTransformation,
    ArithmeticTransformation,
    ComparisonTransformation,
    LogicalTransformation,
    CaseTransformation,
    AggregateTransformation,
    CollectionTransformation,
    WindowFunctionTransformation,
    StructAccessTransformation,
    ArrayAccessTransformation,
    MapAccessTransformation,
    ExtractTransformation,
    WindowBoundaryTransformation,
    MultiComparisonTransformation,
    BetweenComparisonTransformation,
    NullComparisonTransformation,
    ComparisonOperator,
} from '../types';
import { SQLBuilder } from './base';
import { CastBuilder } from './castBuilder';
import { ExpressionBuilder } from './expressionBuilder';
import { WindowSpecBuilder, WindowReferenceBuilder } from './windowSpecBuilder';

export class TransformationBuilder implements SQLBuilder<Transformation> {
    validate (transform: Transformation): string | null {
        switch (transform.type) {
            case TransformType.STRING:
                return this.validateStringTransformation(transform);
            case TransformType.NUMERIC:
                return this.validateNumericTransformation(transform);
            case TransformType.DATE:
                return this.validateDateTransformation(transform);
            case TransformType.CAST:
                return this.validateCastTransformation(transform);
            case TransformType.ARITHMETIC:
                return this.validateArithmeticTransformation(transform);
            case TransformType.COMPARISON:
                return this.validateComparisonTransformation(transform);
            case TransformType.LOGICAL:
                return this.validateLogicalTransformation(transform);
            case TransformType.CASE:
                return this.validateCaseTransformation(transform);
            case TransformType.AGGREGATE:
                return this.validateAggregateTransformation(transform);
            case TransformType.COLLECTION:
                return this.validateCollectionTransformation(transform);
            case TransformType.WINDOW:
                return this.validateWindowFunctionTransformation(transform);
            case TransformType.STRUCT_ACCESS:
                return this.validateStructAccessTransformation(transform);
            case TransformType.ARRAY_ACCESS:
                return this.validateArrayAccessTransformation(transform);
            case TransformType.MAP_ACCESS:
                return this.validateMapAccessTransformation(transform);
            case TransformType.EXTRACT:
                return this.validateExtractTransformation(transform);
            case TransformType.WINDOW_BOUNDARY:
                return this.validateWindowBoundaryTransformation(transform);
            default:
                return `Invalid transformation type: ${(transform as any).type}, expected one of ${Object.values(TransformType).join(', ')} at ${JSON.stringify(transform)}`;
        }
    }

    build (transform: Transformation): string {
        const validation = this.validate(transform);

        if (validation) {
            throw new Error(validation);
        }

        let sql = '';

        switch (transform.type) {
            case TransformType.STRING:
                sql = this.buildStringTransformation(transform);
                break;
            case TransformType.NUMERIC:
                sql = this.buildNumericTransformation(transform);
                break;
            case TransformType.DATE:
                sql = this.buildDateTransformation(transform);
                break;
            case TransformType.CAST:
                sql = this.buildCastTransformation(transform);
                break;
            case TransformType.ARITHMETIC:
                sql = this.buildArithmeticTransformation(transform);
                break;
            case TransformType.COMPARISON:
                sql = this.buildComparisonTransformation(transform);
                break;
            case TransformType.LOGICAL:
                sql = this.buildLogicalTransformation(transform);
                break;
            case TransformType.CASE:
                sql = this.buildCaseTransformation(transform);
                break;
            case TransformType.AGGREGATE:
                sql = this.buildAggregateTransformation(transform);
                break;
            case TransformType.COLLECTION:
                sql = this.buildCollectionTransformation(transform);
                break;
            case TransformType.WINDOW:
                sql = this.buildWindowFunctionTransformation(transform);
                break;
            case TransformType.STRUCT_ACCESS:
                sql = this.buildStructAccessTransformation(transform);
                break;
            case TransformType.ARRAY_ACCESS:
                sql = this.buildArrayAccessTransformation(transform);
                break;
            case TransformType.MAP_ACCESS:
                sql = this.buildMapAccessTransformation(transform);
                break;
            case TransformType.EXTRACT:
                sql = this.buildExtractTransformation(transform);
                break;
            case TransformType.WINDOW_BOUNDARY:
                sql = this.buildWindowBoundaryTransformation(transform);
                break;
            default:
                throw new Error(`Unknown transformation type: ${(transform as any).type} at ${JSON.stringify(transform)}`);
        }

        if (transform.alias) {
            sql += ` AS ${transform.alias}`;
        }

        return sql;
    }

    private validateFunctionParameters (parameters: Expression[]): string | null {
        const invalidParams = parameters.map((param) => new ExpressionBuilder().validate(param)).filter((data) => data !== null);

        return invalidParams.length > 0 ? invalidParams.join(', ') : null;
    }

    private buildFunctionParameters (parameters: Expression[]): string {
        if (parameters.length === 0) {
            return '*';
        }

        return parameters.map((param) => new ExpressionBuilder().build(param)).join(', ');
    }

    private validateStringTransformation (transform: StringTransformation): string | null {
        return this.validateFunctionParameters(transform.parameters);
    }

    private buildStringTransformation (transform: StringTransformation): string {
        return `${transform.function}(${this.buildFunctionParameters(transform.parameters)})`;
    }

    private validateNumericTransformation (transform: NumericTransformation): string | null {
        return this.validateFunctionParameters(transform.parameters);
    }

    private buildNumericTransformation (transform: NumericTransformation): string {
        return `${transform.function}(${this.buildFunctionParameters(transform.parameters)})`;
    }

    private validateDateTransformation (transform: DateTransformation): string | null {
        return this.validateFunctionParameters(transform.parameters);
    }

    private buildDateTransformation (transform: DateTransformation): string {
        return `${transform.function}(${this.buildFunctionParameters(transform.parameters)})`;
    }

    private validateCastTransformation (transform: CastTransformation): string | null {
        return new CastBuilder().validate(transform.targetType) &&
            new ExpressionBuilder().validate(transform.sourceExpression);
    }

    private buildCastTransformation (transform: CastTransformation): string {
        return `CAST(${new ExpressionBuilder().build(transform.sourceExpression)} AS ${new CastBuilder().build(transform.targetType)})`;
    }

    private validateArithmeticTransformation (transform: ArithmeticTransformation): string | null {
        return new ExpressionBuilder().validate(transform.left) &&
            new ExpressionBuilder().validate(transform.right);
    }

    private buildArithmeticTransformation (transform: ArithmeticTransformation): string {
        return `(${new ExpressionBuilder().build(transform.left)} ${transform.operator} ${new ExpressionBuilder().build(transform.right)})`;
    }

    private validateBaseComparisonTransformation (transform: ComparisonTransformation): string | null {
        return new ExpressionBuilder().validate(transform.left) || new ExpressionBuilder().validate(transform.right);
    }

    private buildBaseComparisonTransformation (transform: ComparisonTransformation): string {
        return `${new ExpressionBuilder().build(transform.left)} ${transform.operator} ${new ExpressionBuilder().build(transform.right)}`;
    }

    private validateMultiComparisonTransformation (transform: MultiComparisonTransformation): string | null {
        const otherValidations = transform.right.map((expr) => new ExpressionBuilder().validate(expr))
            .filter((data) => data !== null);

        const rightValidation = otherValidations.length > 0 ? otherValidations.join(', ') : null;


        return new ExpressionBuilder().validate(transform.left) || rightValidation;
    }

    private buildMultiComparisonTransformation (transform: MultiComparisonTransformation): string {
        return `${new ExpressionBuilder().build(transform.left)} ${transform.operator} (${transform.right.map((expr) => new ExpressionBuilder().build(expr)).join(', ')})`;
    }

    private validateBetweenComparisonTransformation (transform: BetweenComparisonTransformation): string | null {
        return new ExpressionBuilder().validate(transform.left) &&
            new ExpressionBuilder().validate(transform.start) &&
            new ExpressionBuilder().validate(transform.end);
    }

    private buildBetweenComparisonTransformation (transform: BetweenComparisonTransformation): string {
        return `${new ExpressionBuilder().build(transform.left)} ${transform.operator} ${new ExpressionBuilder().build(transform.start)} AND ${new ExpressionBuilder().build(transform.end)}`;
    }

    private validateNullComparisonTransformation (transform: NullComparisonTransformation): string | null {
        return new ExpressionBuilder().validate(transform.expression);
    }

    private buildNullComparisonTransformation (transform: NullComparisonTransformation): string {
        return `${new ExpressionBuilder().build(transform.expression)} ${transform.operator}`;
    }

    private validateComparisonTransformation (
        transform: ComparisonTransformation | MultiComparisonTransformation | BetweenComparisonTransformation | NullComparisonTransformation,
    ): string | null {
        switch (transform.operator) {
            case ComparisonOperator.IS_NULL:
            case ComparisonOperator.IS_NOT_NULL:
                return this.validateNullComparisonTransformation(transform);
            case ComparisonOperator.IN:
            case ComparisonOperator.NOT_IN:
                return this.validateMultiComparisonTransformation(transform);
            case ComparisonOperator.BETWEEN:
            case ComparisonOperator.NOT_BETWEEN:
                return this.validateBetweenComparisonTransformation(transform);
            default:
                return this.validateBaseComparisonTransformation(transform);
        }
    }

    private buildComparisonTransformation (
        transform: ComparisonTransformation | MultiComparisonTransformation | BetweenComparisonTransformation | NullComparisonTransformation,
    ): string {
        switch (transform.operator) {
            case ComparisonOperator.IS_NULL:
            case ComparisonOperator.IS_NOT_NULL:
                return this.buildNullComparisonTransformation(transform);
            case ComparisonOperator.IN:
            case ComparisonOperator.NOT_IN:
                return this.buildMultiComparisonTransformation(transform);
            case ComparisonOperator.BETWEEN:
            case ComparisonOperator.NOT_BETWEEN:
                return this.buildBetweenComparisonTransformation(transform);
            default:
                return this.buildBaseComparisonTransformation(transform);
        }
    }

    private validateLogicalTransformation (transform: LogicalTransformation): string | null {
        const exprValidations = transform.expressions.map((expr) => new ExpressionBuilder().validate(expr))
            .filter((data) => data !== null);

        return exprValidations.length > 0 ? exprValidations.join(', ') : null;
    }

    private buildLogicalTransformation (transform: LogicalTransformation): string {
        const exprs = transform.expressions.map((expr) => new ExpressionBuilder().build(expr));

        return `(${exprs.join(` ${transform.operator} `)})`;
    }

    private validateCaseTransformation (transform: CaseTransformation): string | null {
        const whenValidation = transform.conditions.flatMap((cond) => {
            const when = new ExpressionBuilder().validate(cond.when);
            const then = new ExpressionBuilder().validate(cond.then);


            return [when, then];
        });

        const elseValidation = transform.else ? new ExpressionBuilder().validate(transform.else) : 'Invalid condition, else is required';

        return [...whenValidation, elseValidation].filter((data) => data !== null).join(', ');
    }

    private buildCaseTransformation (transform: CaseTransformation): string {
        let sql = 'CASE';

        transform.conditions.forEach((cond) => {
            sql += ` WHEN ${new ExpressionBuilder().build(cond.when)} THEN ${new ExpressionBuilder().build(cond.then)}`;
        });
        if (transform.else) {
            sql += ` ELSE ${new ExpressionBuilder().build(transform.else)}`;
        }
        sql += ' END';

        return sql;
    }

    private validateAggregateTransformation (transform: AggregateTransformation): string | null {
        if (transform.window) {
            if ('spec' in transform.window) {
                return new WindowSpecBuilder().validate(transform.window.spec);
            }

            return new WindowReferenceBuilder().validate(transform.window);
        }

        return this.validateFunctionParameters(transform.parameters);
    }

    private buildAggregateTransformation (transform: AggregateTransformation): string {
        let sql = `${transform.function}(${this.buildFunctionParameters(transform.parameters)})`;

        if (transform.window) {
            if ('spec' in transform.window) {
                sql += ` WINDOW ${new WindowSpecBuilder().build(transform.window.spec)}`;
            } else {
                sql += ` WINDOW ${new WindowReferenceBuilder().build(transform.window)}`;
            }
        }

        return sql;
    }

    private validateCollectionTransformation (transform: CollectionTransformation): string | null {
        return this.validateFunctionParameters(transform.parameters);
    }

    private buildCollectionTransformation (transform: CollectionTransformation): string {
        return `${transform.function}(${this.buildFunctionParameters(transform.parameters)})`;
    }

    private validateWindowFunctionTransformation (transform: WindowFunctionTransformation): string | null {
        if (transform.over && transform.over.partitionBy) {
            const maps = transform.over.partitionBy
                .map((expr) => new ExpressionBuilder().validate(expr))
                .filter((data) => data !== null);

            if (maps.length > 0) {
                return maps.join(', ');
            }
        }

        const data = this.validateFunctionParameters(transform.parameters);

        if (data) {
            return data;
        }

        if (transform.over && transform.over.orderBy) {
            const orders = transform.over.orderBy
                .map((order) => new ExpressionBuilder().validate(order.expression))
                .filter((data) => data !== null);

            if (orders.length > 0) {
                return orders.join(', ');
            }
        }

        if (transform.over && transform.over.alias) {
            if (typeof transform.over.alias !== 'string') {
                return 'Invalid alias';
            }
        }

        return null;
    }

    private buildWindowFunctionTransformation (transform: WindowFunctionTransformation): string {
        let sql = `${transform.function}(${this.buildFunctionParameters(transform.parameters)})`;
        const overClauses: string[] = [];

        if (transform.over && transform.over.partitionBy && transform.over.partitionBy.length > 0) {
            overClauses.push(`PARTITION BY ${transform.over.partitionBy.map((expr) => new ExpressionBuilder().build(expr)).join(', ')}`);
        }

        if (transform.over && transform.over.orderBy && transform.over.orderBy.length > 0) {
            const orderBy = transform.over.orderBy.map((order) => {
                let orderSql = new ExpressionBuilder().build(order.expression);

                if (order.direction) {
                    orderSql += ` ${order.direction}`;
                }
                if (order.nulls) {
                    orderSql += ` ${order.nulls}`;
                }

                return orderSql;
            }).join(', ');

            overClauses.push(`ORDER BY ${orderBy}`);
        }

        if (overClauses.length > 0) {
            sql += ` OVER (${overClauses.join(' ')})`;
        } else if (transform.over && transform.over.alias) {
            sql += ` OVER ${transform.over.alias}`;
        }

        return sql;
    }

    private validateStructAccessTransformation (transform: StructAccessTransformation): string | null {
        return new ExpressionBuilder().validate(transform.struct);
    }

    private buildStructAccessTransformation (transform: StructAccessTransformation): string {
        return `${new ExpressionBuilder().build(transform.struct)}->${transform.field}`;
    }

    private validateArrayAccessTransformation (transform: ArrayAccessTransformation): string | null {
        return new ExpressionBuilder().validate(transform.array) &&
            new ExpressionBuilder().validate(transform.index);
    }

    private buildArrayAccessTransformation (transform: ArrayAccessTransformation): string {
        return `${new ExpressionBuilder().build(transform.array)}[${new ExpressionBuilder().build(transform.index)}]`;
    }

    private validateMapAccessTransformation (transform: MapAccessTransformation): string | null {
        return new ExpressionBuilder().validate(transform.map) &&
            new ExpressionBuilder().validate(transform.key);
    }

    private buildMapAccessTransformation (transform: MapAccessTransformation): string {
        return `${new ExpressionBuilder().build(transform.map)}[${new ExpressionBuilder().build(transform.key)}]`;
    }

    private validateExtractTransformation (transform: ExtractTransformation): string | null {
        return new ExpressionBuilder().validate(transform.source);
    }

    private buildExtractTransformation (transform: ExtractTransformation): string {
        return `EXTRACT(${transform.field} FROM ${new ExpressionBuilder().build(transform.source)})`;
    }

    private validateWindowBoundaryTransformation (transform: WindowBoundaryTransformation): string | null {
        return !transform.window || new WindowReferenceBuilder().validate(transform.window) ? null : 'Invalid window reference';
    }

    private buildWindowBoundaryTransformation (transform: WindowBoundaryTransformation): string {
        let sql: string = transform.boundary;

        if (transform.window) {
            sql += ` ${new WindowReferenceBuilder().build(transform.window)}`;
        }

        return sql;
    }
}
