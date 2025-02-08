import { WindowTimeUnit, WindowType, WindowBoundary } from './enums';

export interface WindowDuration {
    value: number;
    unit: WindowTimeUnit;
}

export interface BaseWindowSpec<T extends WindowType> {
    type: T;
    retention?: WindowDuration;
    gracePeriod?: WindowDuration;
}

export interface TumblingWindowSpec extends BaseWindowSpec<WindowType.TUMBLING> {
    size: WindowDuration;
}

export interface HoppingWindowSpec extends BaseWindowSpec<WindowType.HOPPING> {
    size: WindowDuration;
    advance: WindowDuration;
}

export interface SessionWindowSpec extends BaseWindowSpec<WindowType.SESSION> {
    inactivityGap: WindowDuration;
    sessionConfig?: {
        includeStart?: boolean;
        includeEnd?: boolean;
    };
}

export type WindowSpec = TumblingWindowSpec | HoppingWindowSpec | SessionWindowSpec;

export interface WindowDefinition {
    spec: WindowSpec;
    boundaries?: WindowBoundary[];
}

export interface WindowReference {
    name: string;
    boundaries?: WindowBoundary[];
}
