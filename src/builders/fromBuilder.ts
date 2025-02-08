import {
    FromClause,
    Source,
    StreamJoinWindow,
    BaseJoinCondition,
    StreamSourceOptions,
    TableSourceOptions,
    WindowDuration, DataSourceType, SourceType,
    StreamJoin, TableJoin, StreamTableJoin,
} from '../types';
import { SQLBuilder } from './base';
import { SelectStatementBuilder } from './selectBuilder';
import { WindowSpecBuilder } from './windowSpecBuilder';

export class FromBuilder implements SQLBuilder<FromClause> {
    validate(fromClause: FromClause): string | null {
        if (!this.validateSource(fromClause.source)) {
            return 'Invalid from source';
        }

        if (fromClause.sourceOptions) {
            if (fromClause.sourceType === DataSourceType.STREAM) {
                if (!this.validateStreamSourceOptions(fromClause.sourceOptions as StreamSourceOptions)) {
                    return 'Invalid stream source options';
                }
            } else {
                if (!this.validateTableSourceOptions(fromClause.sourceOptions as TableSourceOptions)) {
                    return 'Invalid table source options';
                }
            }
        }

        if ('joins' in fromClause && fromClause.joins) {
            if (fromClause.sourceType === DataSourceType.STREAM) {
                return fromClause.joins.every(join => this.validateStreamJoin(join)) ? null : 'Invalid stream join';
            } else {
                return fromClause.joins.every(join => this.validateTableJoin(join)) ? null : 'Invalid table join';
            }
        }

        return null;
    }

    build(fromClause: FromClause): string {
        const validation = this.validate(fromClause);
        if (validation) {
            throw new Error(validation);
        }

        let sql = 'FROM ';

        sql += this.buildSource(fromClause.source);

        if (fromClause.sourceOptions) {
            sql += this.buildSourceOptions(fromClause.sourceOptions, fromClause.sourceType);
        }

        if ('joins' in fromClause && fromClause.joins) {
            sql += ' ' + fromClause.joins.map(join => this.buildJoin(join)).join(' ');
        }

        return sql;
    }

    private validateSource(source: Source): boolean {
        switch (source.type) {
            case SourceType.DIRECT:
                return this.validateDirectSource(source);
            case SourceType.SUBQUERY:
                return this.validateSubquerySource(source);
            default:
                return false;
        }
    }

    private validateDirectSource(source: Source & { type: SourceType.DIRECT }): boolean {
        return Boolean(
            source.name &&
            typeof source.name === 'string' &&
            /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(source.name)
        );
    }

    private validateSubquerySource(source: Source & { type: SourceType.SUBQUERY }): boolean {
        return Boolean(
            source.alias &&
            /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(source.alias) &&
            new SelectStatementBuilder().validate(source.query)
        );
    }

    private validateStreamSourceOptions(options: StreamSourceOptions): boolean {
        if (options.partitions && (!Number.isInteger(options.partitions) || options.partitions <= 0)) {
            return false;
        }
        return !(options.replicas && (!Number.isInteger(options.replicas) || options.replicas <= 0));
    }

    private validateTableSourceOptions(options: TableSourceOptions): boolean {
        if (options.partitions && (!Number.isInteger(options.partitions) || options.partitions <= 0)) {
            return false;
        }
        return !(options.replicas && (!Number.isInteger(options.replicas) || options.replicas <= 0));
    }

    private validateStreamJoin(join: StreamJoin | StreamTableJoin): boolean {
        if ('window' in join) {
            if (!this.validateStreamJoinWindow(join.window)) {
                return false;
            }
        }

        return this.validateJoinBase(join);
    }

    private validateTableJoin(join: TableJoin): boolean {
        return this.validateJoinBase(join);
    }

    private validateJoinBase(join: StreamJoin | StreamTableJoin): boolean {
        if (!join.conditions || !Array.isArray(join.conditions) || join.conditions.length === 0) {
            return false;
        }

        return join.conditions.every((condition: BaseJoinCondition) =>
            condition.leftField &&
            condition.rightField
        );
    }

    private validateStreamJoinWindow(window: StreamJoinWindow): boolean {
        return Boolean(
            window.before?.value > 0 &&
            window.after?.value > 0 &&
            (!window.gracePeriod || window.gracePeriod.value > 0)
        );
    }

    private buildSource(source: Source): string {
        switch (source.type) {
            case SourceType.DIRECT:
                return this.buildDirectSource(source);
            case SourceType.SUBQUERY:
                return this.buildSubquerySource(source);
            default:
                throw new Error('Invalid source type');
        }
    }

    private buildDirectSource(source: Source & { type: SourceType.DIRECT }): string {
        let sql = source.name;
        if (source.alias) {
            sql += ` AS ${source.alias}`;
        }
        return sql;
    }

    private buildSubquerySource(source: Source & { type: SourceType.SUBQUERY }): string {
        return `(${new SelectStatementBuilder().build(source.query)}) AS ${source.alias}`;
    }

    private buildSourceOptions(options: StreamSourceOptions | TableSourceOptions, sourceType: DataSourceType): string {
        const optionParts: string[] = [];

        if ('window' in options && options.window) {
            optionParts.push(`WINDOW ${new WindowSpecBuilder().build(options.window)}`);
        }

        if ('partitionBy' in options && options.partitionBy) {
            optionParts.push(`PARTITION BY ${options.partitionBy.join(', ')}`);
        }

        if (options.timestamp) {
            let timestampStr = `TIMESTAMP(${options.timestamp.column}`;
            if (options.timestamp.format) {
                timestampStr += `, '${options.timestamp.format}'`;
            }
            if (options.timestamp.timezone) {
                timestampStr += `, '${options.timestamp.timezone}'`;
            }
            timestampStr += ')';
            optionParts.push(timestampStr);
        }

        return optionParts.length > 0 ? ' ' + optionParts.join(' ') : '';
    }

    private buildJoin(join: StreamJoin | TableJoin | StreamTableJoin): string {
        let sql = `${join.type} JOIN ${join.source.name}`;

        if (join.source.alias) {
            sql += ` AS ${join.source.alias}`;
        }

        if ('window' in join && join.window) {
            sql += ` WITHIN ${this.buildStreamJoinWindow(join.window)}`;
        }

        sql += ' ON ' + join.conditions.map((condition: BaseJoinCondition) =>
            `${condition.leftField} = ${condition.rightField}`
        ).join(' AND ');

        return sql;
    }

    private buildStreamJoinWindow(window: StreamJoinWindow): string {
        let sql = `(${this.buildWindowDuration(window.before)} BEFORE`;

        if (window.after) {
            sql += `, ${this.buildWindowDuration(window.after)} AFTER`;
        }

        if (window.gracePeriod) {
            sql += `, GRACE PERIOD ${this.buildWindowDuration(window.gracePeriod)}`;
        }

        sql += ')';
        return sql;
    }

    private buildWindowDuration(duration: WindowDuration): string {
        return `${duration.value} ${duration.unit}`;
    }
}
