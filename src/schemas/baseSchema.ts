import { z } from 'zod';

import { SerializationFormat } from '../types';

const SOURCE_NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

export const identifierSchema = z.string().regex(SOURCE_NAME_REGEX, 'Identifiers can only contain alphanumeric, dots, underscores, and hyphens');

export const formatConfigSchema = z.object({
    keyFormat: z.nativeEnum(SerializationFormat).optional(),
    valueFormat: z.nativeEnum(SerializationFormat),
    schemaRegistryUrl: z.string().optional(),
    wrapSingleValue: z.boolean().optional(),
});
