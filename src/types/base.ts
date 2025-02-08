import { SerializationFormat } from './enums';

export interface FormatConfig {
    keyFormat?: SerializationFormat;
    valueFormat: SerializationFormat;
    schemaRegistryUrl?: string;
    wrapSingleValue?: boolean;
}
