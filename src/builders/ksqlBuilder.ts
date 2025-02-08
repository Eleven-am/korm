import { SelectType, QueryType, KSQLStatement } from '../types';
import { SQLBuilder } from './base';
import { SelectStatementBuilder } from './selectBuilder';
import {
    InsertQueryBuilder,
    DropStatementBuilder,
    CreateStatementBuilder,
    TerminateQueryBuilder,
    PropertyStatementBuilder,
    ListStatementBuilder,
    ShowStatementBuilder,
    ExplainStatementBuilder,
    DescribeStatementBuilder,
} from './statementBuilder';

export class KSQLStatementBuilder implements SQLBuilder<KSQLStatement> {
    validate(statement: KSQLStatement): string | null {
        switch (statement.type) {
            case SelectType.STAR:
            case SelectType.COLUMN:
                return new SelectStatementBuilder().validate(statement);
            case QueryType.INSERT:
                return new InsertQueryBuilder().validate(statement);
            case QueryType.DROP:
                return new DropStatementBuilder().validate(statement);
            case QueryType.CREATE:
                return new CreateStatementBuilder().validate(statement);
            case QueryType.TERMINATE:
                return new TerminateQueryBuilder().validate(statement);
            case QueryType.PROPERTY:
                return new PropertyStatementBuilder().validate(statement);
            case QueryType.LIST:
                return new ListStatementBuilder().validate(statement);
            case QueryType.SHOW:
                return new ShowStatementBuilder().validate(statement);
            case QueryType.EXPLAIN:
                return new ExplainStatementBuilder().validate(statement);
            case QueryType.DESCRIBE:
                return new DescribeStatementBuilder().validate(statement);
            default:
                return 'Invalid statement type';
        }
    }

    build(statement: KSQLStatement): string {
        const validation = this.validate(statement);
        if (validation) {
            throw new Error(validation);
        }

        switch (statement.type) {
            case SelectType.STAR:
            case SelectType.COLUMN:
                return new SelectStatementBuilder().build(statement);
            case QueryType.INSERT:
                return new InsertQueryBuilder().build(statement);
            case QueryType.DROP:
                return new DropStatementBuilder().build(statement);
            case QueryType.CREATE:
                return new CreateStatementBuilder().build(statement);
            case QueryType.TERMINATE:
                return new TerminateQueryBuilder().build(statement);
            case QueryType.PROPERTY:
                return new PropertyStatementBuilder().build(statement);
            case QueryType.LIST:
                return new ListStatementBuilder().build(statement);
            case QueryType.SHOW:
                return new ShowStatementBuilder().build(statement);
            case QueryType.EXPLAIN:
                return new ExplainStatementBuilder().build(statement);
            case QueryType.DESCRIBE:
                return new DescribeStatementBuilder().build(statement);
            default:
                throw new Error('Invalid statement type');
        }
    }
}
