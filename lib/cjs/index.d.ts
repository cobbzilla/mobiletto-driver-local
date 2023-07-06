/// <reference types="node" />
import { Buffer } from "buffer";
import { MobilettoListOptions, MobilettoMetadata, MobilettoMinimalClient, MobilettoOptions, MobilettoRemoveOptions, MobilettoVisitor, MobilettoWriteSource } from "mobiletto-base";
export declare class StorageClient {
    indexedDB: IDBFactory;
    dbPromise: Promise<IDBDatabase>;
    db: IDBDatabase | null;
    rootStore: IDBObjectStore | null;
    constructor(dbName: string, opts: {
        indexedDB: IDBFactory;
    });
    testConfig: () => Promise<MobilettoMetadata[]>;
    mdb: () => Promise<IDBDatabase>;
    list(pth?: string, optsOrRecursive?: MobilettoListOptions | boolean, visitor?: MobilettoVisitor): Promise<MobilettoMetadata[]>;
    _list(pth?: string, recursive?: boolean, visitor?: MobilettoVisitor): Promise<MobilettoMetadata[]>;
    metadata(path: string): Promise<MobilettoMetadata>;
    write(path: string, generatorOrReadableStream: MobilettoWriteSource): Promise<number>;
    read(path: string, callback: (chunk: Buffer) => void, endCallback?: () => void): Promise<number>;
    remove(path: string, optsOrRecursive?: MobilettoRemoveOptions | boolean, quiet?: boolean): Promise<string[] | string>;
}
export type IdbMobilettoOptions = MobilettoOptions & {
    indexedDB: IDBFactory;
};
export declare const storageClient: (key: string, secret?: string, opts?: IdbMobilettoOptions) => MobilettoMinimalClient;
