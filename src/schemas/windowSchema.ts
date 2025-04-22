import { z } from 'zod';

import { WindowTimeUnit, WindowType, WindowSpec, WindowBoundary } from '../types';

export const windowDurationSchema = z.object({
    value: z.number(),
    unit: z.nativeEnum(WindowTimeUnit),
});

function baseWindowSpec<T extends WindowType> (type: T) {
    return z.object({
        type: z.literal(type),
        retention: windowDurationSchema.optional(),
        gracePeriod: windowDurationSchema.optional(),
    });
}

export const tumblingWindowSpecSchema = baseWindowSpec(WindowType.TUMBLING).merge(z.object({
    size: windowDurationSchema,
}));

export const hoppingWindowSpecSchema = baseWindowSpec(WindowType.HOPPING).merge(z.object({
    size: windowDurationSchema,
    advance: windowDurationSchema,
}));

export const sessionWindowSpecSchema = baseWindowSpec(WindowType.SESSION).merge(z.object({
    inactivityGap: windowDurationSchema,
    sessionConfig: z.object({
        includeStart: z.boolean().optional(),
        includeEnd: z.boolean().optional(),
    }).optional(),
}));

export const windowSpecSchema: z.ZodType<WindowSpec> = z.union([
    tumblingWindowSpecSchema,
    hoppingWindowSpecSchema,
    sessionWindowSpecSchema,
]);

export const windowReferenceSchema = z.object({
    name: z.string(),
    boundaries: z.array(z.nativeEnum(WindowBoundary)).optional(),
});

export const windowDefinitionSchema = z.object({
    spec: windowSpecSchema,
    boundaries: z.array(z.nativeEnum(WindowBoundary)).optional(),
});
