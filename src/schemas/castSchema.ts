import { z } from 'zod';
import { CastType, DataType, CastItem } from '../types';

export const arrayTypeSchema = z.object({
    type: z.literal(CastType.ARRAY),
    elementType: z.nativeEnum(DataType),
})

export const mapTypeSchema = z.object({
    type: z.literal(CastType.MAP),
    valueType: z.nativeEnum(DataType),
})

export const structFieldSchema = z.object({
    name: z.string(),
    type: z.nativeEnum(DataType),
})

export const structTypeSchema = z.object({
    type: z.literal(CastType.STRUCT),
    fields: z.array(structFieldSchema),
})

export const castItemSchema: z.ZodType<CastItem> = z.union([
    z.nativeEnum(DataType),
    arrayTypeSchema,
    mapTypeSchema,
    structTypeSchema,
])
