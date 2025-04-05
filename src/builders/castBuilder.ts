import { ArrayType, CastItem, MapType, StructType, CastType, DataType } from '../types';
import { SQLBuilder } from './base';

export class CastBuilder implements SQLBuilder<CastItem> {
    validate (item: CastItem): string | null {
        if (typeof item === 'string') {
            return this.validateSimpleType(item);
        }

        switch (item.type) {
            case CastType.ARRAY:
                return this.validateArrayType(item);
            case CastType.MAP:
                return this.validateMapType(item);
            case CastType.STRUCT:
                return this.validateStructType(item);
            default:
                return `Invalid cast item type: ${(item as any).type}, expected one of ${Object.values(CastType).join(', ')} at ${JSON.stringify(item)}`;
        }
    }

    build (item: CastItem): string {
        const validation = this.validate(item);

        if (validation) {
            throw new Error(validation);
        }

        if (typeof item === 'string') {
            return this.buildSimpleType(item);
        }

        switch (item.type) {
            case CastType.ARRAY:
                return this.buildArrayType(item);
            case CastType.MAP:
                return this.buildMapType(item);
            case CastType.STRUCT:
                return this.buildStructType(item);
            default:
                throw new Error(`Invalid cast item type: ${(item as any).type}, expected one of ${Object.values(CastType).join(', ')} at ${JSON.stringify(item)}`);
        }
    }

    private buildSimpleType (item: DataType): string {
        return item;
    }

    private validateSimpleType (item: DataType): string | null {
        return [
            DataType.BOOLEAN,
            DataType.INTEGER,
            DataType.BIGINT,
            DataType.DOUBLE,
            DataType.STRING,
            DataType.DATE,
            DataType.TIME,
            DataType.TIMESTAMP,
            DataType.DECIMAL,
            DataType.INTERVAL,
        ].includes(item)
            ? null
            : `Invalid simple type: ${item}, expected one of ${Object.values(DataType).join(', ')}`;
    }

    private buildArrayType (item: ArrayType): string {
        return `ARRAY<${this.build(item.elementType)}>`;
    }

    private validateArrayType (item: ArrayType): string | null {
        return this.validate(item.elementType) ? null : `Invalid array type for element: ${item.elementType}`;
    }

    private buildMapType (item: MapType): string {
        return `MAP<STRING, ${this.build(item.valueType)}>`;
    }

    private validateMapType (item: MapType): string | null {
        return this.validate(item.valueType) && item.valueType === DataType.STRING ? null : `Invalid map type for value: ${item.valueType}`;
    }

    private buildStructType (item: StructType): string {
        const fields = item.fields.map((field) => `${field.name} ${this.build(field.type)}`).join(', ');

        return `STRUCT<${fields}>`;
    }

    private validateStructType (item: StructType): string | null {
        const validationErrors = item.fields.map((field) => this.validate(field.type)).filter((error) => error !== null);

        if (validationErrors.length > 0) {
            return `Invalid struct type: ${validationErrors.join(', ')}`;
        }

        return item.fields.length > 0 ? null : 'Struct must have at least one field';
    }
}
