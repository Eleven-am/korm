import {
    CreateAsSelectStatement,
    CreateSourceStatement,
    CreateStatement,
    DescribeQuery,
    DropStatement,
    ExplainQuery,
    InsertQuery,
    ListStatement,
    PropertyStatement,
    ShowStatement,
    ShowType,
    TerminateQuery,
    InsertValueQuery,
    InsertSelectQuery,
    CreateType,
    DataSourceType,
    PropertyAction,
    QueryType,
    InsertType,
} from '../types';
import { SQLBuilder } from './base';
import { CastBuilder } from './castBuilder';
import { SelectStatementBuilder } from './selectBuilder';

export class TerminateQueryBuilder implements SQLBuilder<TerminateQuery> {
    validate (query: TerminateQuery): string | null {
        if (query.type !== QueryType.TERMINATE) {
            return 'Invalid query type';
        }

        if (!query.queryId || query.queryId.trim() === '') {
            return 'Invalid query ID';
        }

        const queryIdRegex = /^[a-zA-Z0-9_-]+$/;

        return queryIdRegex.test(query.queryId) ? null : 'Invalid query ID';
    }

    build (query: TerminateQuery): string {
        const validation = this.validate(query);

        if (validation) {
            throw new Error(validation);
        }

        return `TERMINATE ${query.queryId};`;
    }
}

export class PropertyStatementBuilder implements SQLBuilder<PropertyStatement> {
    validate (statement: PropertyStatement): string | null {
        if (statement.type !== QueryType.PROPERTY) {
            return 'Invalid query type';
        }

        if (!statement.property || statement.property.trim() === '') {
            return 'Invalid property name';
        }

        if (statement.action === PropertyAction.SET && !statement.value) {
            return 'SET action requires a value';
        }

        if (statement.action === PropertyAction.SHOW && statement.value !== undefined) {
            return 'SHOW action does not require a value';
        }

        const propertyNameRegex = /^[a-zA-Z0-9._-]+$/;

        return propertyNameRegex.test(statement.property) ? null : 'Invalid property name';
    }

    build (statement: PropertyStatement): string {
        const validation = this.validate(statement);

        if (validation) {
            throw new Error(validation);
        }

        switch (statement.action) {
            case PropertyAction.SET:
                return `SET '${statement.property}'=${this.formatPropertyValue(statement.value!)};`;

            case PropertyAction.SHOW:
                return `SHOW ${statement.property};`;

            default:
                throw new Error(`Unsupported property action: ${statement.action}`);
        }
    }

    private formatPropertyValue (value: string): string {
        if ((/[\s'"`{}()[\],;]/).test(value)) {
            const escapedValue = value.replace(/'/g, '\'\'');

            return `'${escapedValue}'`;
        }

        return value;
    }
}

export class ListStatementBuilder implements SQLBuilder<ListStatement> {
    validate (statement: ListStatement): string | null {
        if (statement.type !== QueryType.LIST) {
            return 'Invalid query type';
        }

        return [DataSourceType.STREAM, DataSourceType.TABLE].includes(statement.sourceType) ? null : 'Invalid source type';
    }

    build (statement: ListStatement): string {
        const validation = this.validate(statement);

        if (validation) {
            throw new Error(validation);
        }

        let sql = `LIST ${this.pluralizeSourceType(statement.sourceType)}`;

        if (statement.extended) {
            sql += ' EXTENDED';
        }

        return `${sql};`;
    }

    private pluralizeSourceType (sourceType: DataSourceType): string {
        switch (sourceType) {
            case DataSourceType.STREAM:
                return 'STREAMS';
            case DataSourceType.TABLE:
                return 'TABLES';
            default:
                throw new Error(`Unsupported source type for LIST: ${sourceType}`);
        }
    }
}

export class ShowStatementBuilder implements SQLBuilder<ShowStatement> {
    validate (statement: ShowStatement): string | null {
        if (statement.type !== QueryType.SHOW) {
            return 'Invalid query type';
        }

        return Object.values(ShowType)
            .includes(statement.showType)
            ? null
            : 'Invalid show type';
    }

    build (statement: ShowStatement): string {
        const validation = this.validate(statement);

        if (validation) {
            throw new Error(validation);
        }

        return `SHOW ${statement.showType};`;
    }
}

export class DescribeStatementBuilder implements SQLBuilder<DescribeQuery> {
    validate (statement: DescribeQuery): string | null {
        if (statement.type !== QueryType.DESCRIBE) {
            return 'Invalid query type';
        }

        if (!statement.target || !statement.target.name || !statement.target.type) {
            return 'Invalid target';
        }

        if (![DataSourceType.STREAM, DataSourceType.TABLE].includes(statement.target.type)) {
            return 'Invalid target type';
        }

        const validNameRegex = /^[a-zA-Z][a-zA-Z0-9_]*(\.[a-zA-Z][a-zA-Z0-9_]*)*$/;

        return validNameRegex.test(statement.target.name) ? null : 'Invalid target name';
    }

    build (statement: DescribeQuery): string {
        const validation = this.validate(statement);

        if (validation) {
            throw new Error(validation);
        }

        let sql = `DESCRIBE ${statement.target.type} ${statement.target.name}`;

        if (statement.extended) {
            sql += ' EXTENDED';
        }

        return `${sql};`;
    }
}

export class DropStatementBuilder implements SQLBuilder<DropStatement> {
    validate (statement: DropStatement): string | null {
        if (statement.type !== QueryType.DROP) {
            return 'Invalid query type';
        }

        if (![DataSourceType.STREAM, DataSourceType.TABLE].includes(statement.sourceType)) {
            return 'Invalid source type';
        }

        return !(!statement.sourceName || !this.validateSourceName(statement.sourceName)) ? null : 'Invalid source name';
    }

    build (statement: DropStatement): string {
        const validation = this.validate(statement);

        if (validation) {
            throw new Error(validation);
        }

        let sql = 'DROP';

        sql += ` ${statement.sourceType}`;

        if (statement.options?.ifExists) {
            sql += ' IF EXISTS';
        }

        sql += ` ${statement.sourceName}`;

        if (statement.options?.deleteTopic) {
            sql += ' DELETE TOPIC';
        }

        return `${sql};`;
    }

    private validateSourceName (name: string): string | null {
        const validNameRegex = /^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$/;


        return validNameRegex.test(name) ? null : 'Invalid source name';
    }
}

export class CreateStatementBuilder implements SQLBuilder<CreateStatement> {
    private readonly selectBuilder: SelectStatementBuilder;

    private readonly castBuilder: CastBuilder;

    constructor () {
        this.selectBuilder = new SelectStatementBuilder();
        this.castBuilder = new CastBuilder();
    }

    validate (statement: CreateStatement): string | null {
        if (statement.type !== QueryType.CREATE) {
            return 'Invalid query type';
        }

        const result = this.validateSourceName(statement.sourceName);

        if (result !== null) {
            return result;
        }

        if (![DataSourceType.STREAM, DataSourceType.TABLE].includes(statement.sourceType)) {
            return 'Invalid source type';
        }

        if (statement.createType === CreateType.SOURCE) {
            return this.validateSourceCreation(statement);
        } else if (statement.createType === CreateType.AS_SELECT) {
            return this.validateAsSelectCreation(statement);
        }

        return null;
    }

    build (statement: CreateStatement): string {
        const validation = this.validate(statement);

        if (validation) {
            throw new Error(validation);
        }

        let sql = this.buildCreatePrefix(statement);

        if (statement.createType === CreateType.SOURCE) {
            sql += this.buildSourceCreation(statement);
        } else {
            sql += this.buildAsSelectCreation(statement);
        }

        return `${sql};`;
    }

    private validateSourceName (name: string): string | null {
        const validNameRegex = /^[a-zA-Z_][a-zA-Z0-9_]*$/;


        return validNameRegex.test(name) ? null : 'Invalid source name';
    }

    private validateSourceCreation (statement: CreateSourceStatement<DataSourceType>): string | null {
        if (!statement.schema || statement.schema.length === 0) {
            return 'Schema is required';
        }

        for (const field of statement.schema) {
            if (!field.name || !field.type || !this.castBuilder.validate(field.type)) {
                return `Invalid schema field: ${field.name}`;
            }
        }

        return this.validateCreateOptions(statement);
    }

    private validateAsSelectCreation (statement: CreateAsSelectStatement<DataSourceType>): string | null {
        if (!statement.select) {
            return 'Invalid select statement, select is required';
        }

        const selectValidation = this.selectBuilder.validate(statement.select);

        if (selectValidation) {
            return selectValidation;
        }

        return this.validateCreateOptions(statement);
    }

    private validateCreateOptions (statement: CreateStatement): string | null {
        const { options } = statement;

        if (!options.format || !options.format.valueFormat) {
            return 'Format is required';
        }

        if (options.partitions !== undefined && (!Number.isInteger(options.partitions) || options.partitions <= 0)) {
            return 'Invalid partitions';
        }
        if (options.replicas !== undefined && (!Number.isInteger(options.replicas) || options.replicas <= 0)) {
            return 'Invalid replicas';
        }

        if (statement.sourceType === DataSourceType.TABLE) {
            if ('caching' in options && typeof options.caching !== 'boolean') {
                return 'Invalid caching';
            }
        }

        return null;
    }

    private buildCreatePrefix (statement: CreateStatement): string {
        let sql = 'CREATE';

        if (statement.ifNotExists) {
            sql += ' IF NOT EXISTS';
        }

        sql += ` ${statement.sourceType} ${statement.sourceName}`;

        return sql;
    }

    private buildSourceCreation (statement: CreateSourceStatement<DataSourceType>): string {
        const schemaFields = statement.schema.map((field) => {
            const fieldDef = `${field.name} ${this.castBuilder.build(field.type)}`;


            return field.key ? `${fieldDef} KEY` : fieldDef;
        });

        let sql = ` (${schemaFields.join(', ')})`;

        sql += this.buildCreateOptions(statement);

        return sql;
    }

    private buildAsSelectCreation (statement: CreateAsSelectStatement<DataSourceType>): string {
        let sql = this.buildCreateOptions(statement);

        sql += ` AS ${this.selectBuilder.build(statement.select)}`;

        return sql;
    }

    private buildCreateOptions (statement: CreateStatement): string {
        const options: string[] = [];

        const { format } = statement.options;

        options.push(`VALUE_FORMAT='${format.valueFormat}'`);
        if (format.keyFormat) {
            options.push(`KEY_FORMAT='${format.keyFormat}'`);
        }
        if (format.schemaRegistryUrl) {
            options.push(`SCHEMA_REGISTRY_URL='${format.schemaRegistryUrl}'`);
        }

        if (statement.options.kafkaTopic) {
            options.push(`KAFKA_TOPIC='${statement.options.kafkaTopic}'`);
        }

        if (statement.options.partitions) {
            options.push(`PARTITIONS=${statement.options.partitions}`);
        }
        if (statement.options.replicas) {
            options.push(`REPLICAS=${statement.options.replicas}`);
        }

        if (statement.options.timestampColumn) {
            options.push(`TIMESTAMP='${statement.options.timestampColumn.name}'`);
            if (statement.options.timestampColumn.format) {
                options.push(`TIMESTAMP_FORMAT='${statement.options.timestampColumn.format}'`);
            }
        }

        if (statement.sourceType === DataSourceType.TABLE) {
            if ('stateStoreName' in statement.options && statement.options.stateStoreName) {
                options.push(`STATE_STORE_NAME='${statement.options.stateStoreName}'`);
            }
            if ('caching' in statement.options && statement.options.caching !== undefined) {
                options.push(`CACHING=${statement.options.caching}`);
            }
        }

        return options.length > 0 ? ` WITH (${options.join(', ')})` : '';
    }
}

export class InsertQueryBuilder implements SQLBuilder<InsertQuery> {
    private readonly selectBuilder: SelectStatementBuilder;

    private readonly castBuilder: CastBuilder;

    constructor () {
        this.selectBuilder = new SelectStatementBuilder();
        this.castBuilder = new CastBuilder();
    }

    validate (query: InsertQuery): string | null {
        if (query.type !== QueryType.INSERT) {
            return 'Invalid query type';
        }

        if (!this.validateTarget(query.statement.target)) {
            return 'Invalid target';
        }

        if (query.statement.schema && !this.validateSchema(query.statement.schema)) {
            return 'Invalid schema';
        }

        if (query.statement.type === InsertType.VALUES) {
            return this.validateValuesInsert(query.statement);
        } else if (query.statement.type === InsertType.SELECT) {
            return this.validateSelectInsert(query.statement);
        }

        return null;
    }

    build (query: InsertQuery): string {
        const validation = this.validate(query);

        if (validation) {
            throw new Error(validation);
        }

        let sql = this.buildInsertPrefix(query.statement);

        if (query.statement.type === InsertType.VALUES) {
            sql += this.buildValuesInsert(query.statement);
        } else {
            sql += this.buildSelectInsert(query.statement);
        }

        return `${sql};`;
    }

    private validateTarget (target: { name: string, columns?: string[] }): string | null {
        const validNameRegex = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

        if (!validNameRegex.test(target.name)) {
            return 'Invalid target name';
        }

        if (target.columns) {
            if (target.columns.length === 0) {
                return 'Target columns are required';
            }

            return target.columns.every((col) => validNameRegex.test(col)) ? null : 'Invalid target columns';
        }

        return null;
    }

    private validateSchema (schema: { fields: Array<{ name: string, type: any, required?: boolean }> }): string | null {
        return schema.fields.every((field) => {
            const validNameRegex = /^[a-zA-Z_][a-zA-Z0-9_]*$/;


            return (
                validNameRegex.test(field.name) &&
                this.castBuilder.validate(field.type)
            );
        })
            ? null
            : 'Invalid schema';
    }

    private validateValuesInsert (statement: InsertValueQuery): string | null {
        // Must have at least one value
        if (!statement.data || statement.data.length === 0) {
            return 'No values to insert';
        }

        // Each value must have a column and valid literal
        return statement.data.every((value) => {
            const validNameRegex = /^[a-zA-Z_][a-zA-Z0-9_]*$/;


            return (
                validNameRegex.test(value.column) &&
                value.value !== undefined &&
                (value.value.value === null || ['string', 'number', 'boolean'].includes(typeof value.value.value))
            );
        })
            ? null
            : 'Invalid values';
    }

    private validateSelectInsert (statement: InsertSelectQuery): string | null {
        return this.selectBuilder.validate(statement.data);
    }

    private buildInsertPrefix (statement: InsertValueQuery | InsertSelectQuery): string {
        let sql = 'INSERT INTO';

        sql += ` ${statement.target.name}`;

        if (statement.target.columns?.length) {
            sql += ` (${statement.target.columns.join(', ')})`;
        }

        return sql;
    }

    private buildValuesInsert (statement: InsertValueQuery): string {
        const columns = statement.data.map((v) => v.column).join(', ');
        const values = statement.data.map((v) => {
            const value = v.value.value;

            if (value === null) {
                return 'NULL';
            }
            if (typeof value === 'string') {
                return `'${value.replace(/'/g, '\'\'')}'`;
            }

            return String(value);
        }).join(', ');

        return ` (${columns}) VALUES (${values})`;
    }

    private buildSelectInsert (statement: InsertSelectQuery): string {
        return ` ${this.selectBuilder.build(statement.data)}`;
    }
}

export class ExplainStatementBuilder implements SQLBuilder<ExplainQuery> {
    validate (statement: ExplainQuery): string | null {
        if (statement.type !== QueryType.EXPLAIN) {
            return 'Invalid query type';
        }

        if (!statement.statement) {
            return 'Missing statement';
        }

        try {
            if (statement.statement.type === QueryType.CREATE) {
                return this.validateCreateStatement(statement.statement);
            }

            return new SelectStatementBuilder().validate(statement.statement);
        } catch (error) {
            return error instanceof Error ? error.message : 'Unknown error';
        }
    }

    build (statement: ExplainQuery): string {
        const validation = this.validate(statement);

        if (validation) {
            throw new Error(validation);
        }

        try {
            let innerSql: string;

            if (statement.statement.type === QueryType.CREATE) {
                innerSql = new CreateStatementBuilder().build(statement.statement);
            } else {
                innerSql = new SelectStatementBuilder().build(statement.statement);
            }

            return `EXPLAIN ${statement.analyze ? 'ANALYZE ' : ''}${innerSql}`;
        } catch (error) {
            throw new Error(`Failed to build EXPLAIN statement: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
    }

    private validateCreateStatement (statement: CreateStatement): string | null {
        if (statement.createType !== 'AS_SELECT') {
            return 'Only AS SELECT create statements are supported';
        }

        return new CreateStatementBuilder().validate(statement);
    }
}
