import { DataType, CastType } from './enums';

export interface ArrayType<DataT extends DataType = DataType> {
    type: CastType.ARRAY;
    elementType: DataT;
}

export interface MapType<DataT extends DataType = DataType> {
    type: CastType.MAP;
    valueType: DataT;
}

export interface StructField <DataT extends DataType = DataType> {
    name: string;
    type: DataT;
}

export interface StructType<DataT extends DataType = DataType> {
    type: CastType.STRUCT;
    fields: StructField<DataT>[];
}

export type CastItem<DataT extends DataType = DataType> = DataT | ArrayType<DataT> | MapType<DataT> | StructType<DataT>;
