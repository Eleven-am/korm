import { NonAggregateExpression } from './expressions';

export interface PartitionBy{
    columns: NonAggregateExpression[];
}
