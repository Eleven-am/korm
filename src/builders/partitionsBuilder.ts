import { PartitionBy } from '../types';
import { SQLBuilder } from './base';
import { ExpressionBuilder } from './expressionBuilder';

export class PartitionBuilder implements SQLBuilder<PartitionBy> {
    validate (partition: PartitionBy): string | null {
        const validations = partition.columns
            .map((col) => new ExpressionBuilder().validate(col))
            .filter((validation) => validation !== null);

        if (validations.length > 0) {
            return `Invalid partition by column: ${validations.join(', ')}`;
        }

        return null;
    }

    build (partition: PartitionBy): string {
        const validation = this.validate(partition);

        if (validation) {
            throw new Error(validation);
        }

        const columns = partition.columns
            .map((col) => new ExpressionBuilder().build(col))
            .join(', ');


        return `PARTITION BY ${columns}`;
    }
}
