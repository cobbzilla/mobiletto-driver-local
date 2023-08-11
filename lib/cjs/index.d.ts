/// <reference types="node" />
/// <reference types="node" />
import { MobilettoError, MobilettoNotFoundError, MobilettoMetadata, MobilettoRemoveOptions, MobilettoVisitor, MobilettoWriteSource, MobilettoOptions, MobilettoDriverInfo } from "mobiletto-base";
import * as fs from "fs";
export type LocalDriverOptions = MobilettoOptions & {
    fileMode?: string;
    dirMode?: string;
    createIfNotExist?: boolean;
};
export type FsMetadata = MobilettoMetadata & {
    link?: string;
};
export declare const LocalInfo: MobilettoDriverInfo;
declare class StorageClient {
    baseDir: string;
    fileMode: string;
    dirMode: string;
    constructor(baseDir: string, opts?: LocalDriverOptions);
    resolveSymlinks(path: string): {
        path: string;
        stat: fs.Stats;
        symlinksFollowed: boolean;
    };
    testConfig: () => Promise<MobilettoMetadata[] | undefined>;
    info: () => MobilettoDriverInfo;
    normalizePath: (path: string) => string;
    denormalizePath: (path: string) => string;
    ioError: (err: any, path: string, method: string) => MobilettoError | MobilettoNotFoundError;
    fileToObject: (dir: string) => (f: string) => FsMetadata;
    readDirFiles: (dir: string, recursive: boolean, visitor: MobilettoVisitor) => Promise<FsMetadata[] | undefined>;
    dirFiles: (dir: string, visitor: MobilettoVisitor) => Promise<FsMetadata[]>;
    list(pth?: string, recursive?: boolean, visitor?: null): Promise<MobilettoMetadata[] | undefined>;
    metadata(path: string): Promise<MobilettoMetadata>;
    mkdirs(path: string): void;
    normalizePathAndEnsureParentDirs(path: string): Promise<string>;
    write(path: string, generatorOrReadableStream: MobilettoWriteSource): Promise<number>;
    read(path: string, callback: (chunk: Buffer) => void, endCallback?: () => void): Promise<number>;
    remove(path: string, optsOrRecursive?: MobilettoRemoveOptions | boolean, quiet?: boolean): Promise<string | string[]>;
}
export declare const storageClient: (key: string, secret: string, opts: MobilettoOptions) => StorageClient;
export {};
