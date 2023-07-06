"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.storageClient = exports.StorageClient = void 0;
const buffer_1 = require("buffer");
const mobiletto_common_1 = require("mobiletto-common");
const IDB_SCHEMA_VERSION = 1;
const ROOT_STORE = "rootStore";
class StorageClient {
    constructor(dbName, opts) {
        this.db = null;
        this.rootStore = null;
        this.testConfig = () => __awaiter(this, void 0, void 0, function* () { return yield this.list(); });
        this.mdb = () => __awaiter(this, void 0, void 0, function* () {
            if (this.db)
                return this.db;
            const db = yield this.dbPromise;
            if (db)
                return db;
            if (this.db)
                return this.db;
            throw new mobiletto_common_1.MobilettoError(`mdb: error getting database`);
        });
        if (!dbName) {
            throw new mobiletto_common_1.MobilettoError("indexeddb.StorageClient: key (dbName) is required");
        }
        if (opts && opts.indexedDB) {
            this.indexedDB = opts.indexedDB;
        }
        else {
            throw new mobiletto_common_1.MobilettoError("indexeddb.StorageClient: opts.indexedDB is required");
        }
        this.dbPromise = new Promise((resolve, reject) => {
            const openOrCreateDB = this.indexedDB.open(dbName, IDB_SCHEMA_VERSION);
            openOrCreateDB.onerror = () => {
                reject("indexedDB.open failed");
            };
            openOrCreateDB.onsuccess = () => {
                resolve((this.db = openOrCreateDB.result));
            };
            openOrCreateDB.onupgradeneeded = () => {
                if (!this.db) {
                    reject(new mobiletto_common_1.MobilettoError("indexedDB.upgradeneeded: this.db was never initialized"));
                    return;
                }
                this.db.onerror = () => {
                    reject(new mobiletto_common_1.MobilettoError("indexedDB.open failed (upgradeneeded)"));
                };
                this.rootStore = this.db.createObjectStore(ROOT_STORE, {});
            };
        });
    }
    list(pth, optsOrRecursive, visitor) {
        return __awaiter(this, void 0, void 0, function* () {
            const recursive = optsOrRecursive === true || (optsOrRecursive && optsOrRecursive.recursive) || false;
            return this._list(pth, recursive, visitor);
        });
    }
    _list(pth = "", recursive = false, visitor) {
        return __awaiter(this, void 0, void 0, function* () {
            const db = yield this.mdb();
            return new Promise((resolve, reject) => {
                try {
                    const listTx = db.transaction(ROOT_STORE, "readonly");
                    listTx.oncomplete = () => {
                        mobiletto_common_1.logger.info(`list(${pth}) listTx completed`);
                    };
                    listTx.onerror = () => {
                        mobiletto_common_1.logger.error(`list(${pth}) listTx error`);
                    };
                    const listRequest = listTx.objectStore(ROOT_STORE).getAll();
                    listRequest.onerror = (event) => {
                        reject(event);
                    };
                    listRequest.onsuccess = () => __awaiter(this, void 0, void 0, function* () {
                        let foundExact = null;
                        const filtered = listRequest.result
                            .sort((o1, o2) => o1.name.localeCompare(o2.name))
                            .map((o) => {
                            const prefixMatch = o.name.startsWith(pth);
                            if (o.name === pth) {
                                foundExact = o;
                                return o;
                            }
                            if (prefixMatch) {
                                if (recursive)
                                    return o;
                                const normMatch = pth === "" ? "" : pth.endsWith("/") ? pth : pth + "/";
                                if (o.name.startsWith(normMatch) && o.name.length > normMatch.length) {
                                    const nextSlash = o.name.indexOf("/", normMatch.length + 1);
                                    if (nextSlash === -1)
                                        return o;
                                    const dir = {
                                        type: mobiletto_common_1.M_DIR,
                                        name: o.name.substring(0, nextSlash),
                                    };
                                    return dir;
                                }
                            }
                            return null;
                        })
                            .filter((o) => o != null)
                            .filter((value, index, self) => value != null && index === self.findIndex((t) => t.name === value.name));
                        if (foundExact && !recursive) {
                            if (visitor)
                                yield visitor(foundExact);
                            resolve([foundExact]);
                        }
                        else {
                            if (visitor) {
                                for (const f of filtered.filter((o) => o.type === mobiletto_common_1.M_FILE)) {
                                    yield visitor(f);
                                }
                            }
                            resolve(filtered);
                        }
                    });
                }
                catch (e) {
                    mobiletto_common_1.logger.error(`list(${pth}): error ${e}`);
                }
            });
        });
    }
    metadata(path) {
        return __awaiter(this, void 0, void 0, function* () {
            const db = yield this.mdb();
            let data = null;
            return new Promise((resolve, reject) => {
                const readMetaTx = db.transaction(ROOT_STORE, "readonly");
                readMetaTx.oncomplete = () => {
                    if (!data) {
                        reject(new mobiletto_common_1.MobilettoNotFoundError(path));
                        return;
                    }
                    try {
                        delete data.bytes;
                        resolve(data);
                    }
                    catch (e) {
                        reject(new mobiletto_common_1.MobilettoError(`metadata(${path}) error parsing data: ${e}`));
                    }
                };
                readMetaTx.onerror = () => {
                    reject(new mobiletto_common_1.MobilettoError(`metadata(${path}) readMetaTx error`));
                };
                const readRequest = readMetaTx.objectStore(ROOT_STORE).get(path);
                readRequest.onerror = () => {
                    reject(new mobiletto_common_1.MobilettoError(`metadata(${path}) readRequest error`));
                };
                readRequest.onsuccess = () => {
                    data = readRequest.result;
                };
            });
        });
    }
    write(path, generatorOrReadableStream) {
        return __awaiter(this, void 0, void 0, function* () {
            const bytes = [];
            const db = yield this.mdb();
            /* eslint-disable no-async-promise-executor */
            return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                /* eslint-enable no-async-promise-executor */
                if ((0, mobiletto_common_1.isReadable)(generatorOrReadableStream)) {
                    const readable = generatorOrReadableStream;
                    /* eslint-disable @typescript-eslint/no-explicit-any */
                    const streamHandler = (stream) => new Promise((resolve2, reject2) => {
                        stream.on("data", (data) => {
                            /* eslint-enable @typescript-eslint/no-explicit-any */
                            if (data) {
                                bytes.push(...data);
                            }
                        });
                        stream.on("error", (e) => {
                            reject2(e);
                        });
                        stream.on("end", () => {
                            resolve2(0);
                        });
                    });
                    yield streamHandler(readable);
                }
                else {
                    const generator = generatorOrReadableStream;
                    let chunk = (0, mobiletto_common_1.isAsyncGenerator)(generator) ? (yield generator.next()).value : generator.next().value;
                    let nullCount = 0;
                    while (chunk || nullCount < 5) {
                        if (chunk) {
                            bytes.push(...chunk);
                        }
                        else {
                            nullCount++;
                        }
                        chunk = (0, mobiletto_common_1.isAsyncGenerator)(generator) ? (yield generator.next()).value : generator.next().value;
                    }
                }
                const writeTx = db.transaction(ROOT_STORE, "readwrite");
                writeTx.oncomplete = () => {
                    mobiletto_common_1.logger.info(`write(${path}) writeTx complete`);
                    resolve(bytes.length);
                };
                writeTx.onerror = () => {
                    reject(new mobiletto_common_1.MobilettoError(`write(${path}) writeTx error`));
                };
                const writeObject = {
                    name: path,
                    type: mobiletto_common_1.M_FILE,
                    bytes,
                    size: bytes.length,
                    mtime: Date.now(),
                };
                const writeRequest = writeTx.objectStore(ROOT_STORE).put(writeObject, path);
                writeRequest.onerror = () => {
                    reject(new mobiletto_common_1.MobilettoError(`write(${path}) writeRequest error`));
                };
                writeRequest.onsuccess = () => {
                    mobiletto_common_1.logger.info(`write(${path}) writeRequest success`);
                    resolve(bytes.length);
                };
            }));
        });
    }
    read(path, callback, endCallback) {
        return __awaiter(this, void 0, void 0, function* () {
            const db = yield this.mdb();
            let bytesRead = 0;
            return new Promise((resolve, reject) => {
                const readTx = db.transaction(ROOT_STORE, "readonly");
                let notFound = null;
                readTx.oncomplete = () => {
                    if (bytesRead) {
                        resolve(bytesRead);
                    }
                    else if (notFound) {
                        reject(notFound);
                    }
                    else {
                        mobiletto_common_1.logger.warn(`read(${path}) readTx complete but neither bytesRead nor notFound was set`);
                    }
                };
                readTx.onerror = () => {
                    reject(new mobiletto_common_1.MobilettoError(`read(${path}) readTx error`));
                };
                const readRequest = readTx.objectStore(ROOT_STORE).get(path);
                readRequest.onerror = () => {
                    reject(new mobiletto_common_1.MobilettoError(`read(${path}) readRequest error`));
                };
                readRequest.onsuccess = () => {
                    if (typeof readRequest.result === "undefined") {
                        notFound = new mobiletto_common_1.MobilettoNotFoundError(path);
                        return;
                    }
                    if (readRequest.result && readRequest.result.bytes && readRequest.result.bytes.length > 0) {
                        let bytes;
                        if (typeof readRequest.result.bytes[0] === "string") {
                            bytes = buffer_1.Buffer.from(readRequest.result.bytes.join(""));
                        }
                        else {
                            bytes = buffer_1.Buffer.from(readRequest.result.bytes);
                        }
                        callback(bytes);
                        if (endCallback)
                            endCallback();
                        bytesRead = bytes.length;
                    }
                };
            });
        });
    }
    remove(path, optsOrRecursive, quiet) {
        return __awaiter(this, void 0, void 0, function* () {
            const recursive = optsOrRecursive === true || (optsOrRecursive && optsOrRecursive.recursive);
            const db = yield this.mdb();
            const deleteErrors = [];
            const deletedFiles = [];
            const deleteVisitor = (file) => __awaiter(this, void 0, void 0, function* () {
                return yield new Promise((resolve, reject) => {
                    const path = file.name;
                    if (file.type !== mobiletto_common_1.M_FILE) {
                        mobiletto_common_1.logger.warn(`remove(${path}): not a file`);
                        if (quiet) {
                            resolve(true);
                        }
                        else {
                            const err = new mobiletto_common_1.MobilettoError(`remove(${path}): not a file`);
                            deleteErrors.push(err);
                            reject(err);
                        }
                    }
                    let deleteResult = null;
                    const deleteTx = db.transaction(ROOT_STORE, "readwrite");
                    deleteTx.oncomplete = () => {
                        if (deleteResult == null) {
                            if (quiet) {
                                mobiletto_common_1.logger.warn(`remove(${path}): deleteResult was null`);
                                resolve(true);
                            }
                            else {
                                const err = new mobiletto_common_1.MobilettoError(`remove(${path}): deleteResult was null`);
                                deleteErrors.push(err);
                                reject(err);
                            }
                        }
                        else {
                            if (deleteResult instanceof mobiletto_common_1.MobilettoNotFoundError || deleteResult instanceof mobiletto_common_1.MobilettoError) {
                                if (quiet) {
                                    resolve(true);
                                }
                                else {
                                    deleteErrors.push(deleteResult);
                                    reject(deleteResult);
                                }
                            }
                            else {
                                deletedFiles.push(deleteResult);
                                resolve(deleteResult);
                            }
                        }
                    };
                    deleteTx.onerror = () => {
                        if (quiet) {
                            mobiletto_common_1.logger.warn(`remove(${path}): deleteTx error`);
                            resolve(true);
                        }
                        else {
                            const err = new mobiletto_common_1.MobilettoError(`remove(${path}) deleteTx error`);
                            deleteErrors.push(err);
                            reject(err);
                        }
                    };
                    const findRequest = deleteTx.objectStore(ROOT_STORE).get(path);
                    findRequest.onerror = () => {
                        mobiletto_common_1.logger.warn(`remove(${path}): error`);
                        deleteResult = new mobiletto_common_1.MobilettoNotFoundError(path);
                    };
                    findRequest.onsuccess = () => {
                        if (typeof findRequest.result === "undefined") {
                            deleteResult = new mobiletto_common_1.MobilettoNotFoundError(path);
                        }
                        else {
                            const deleteRequest = deleteTx.objectStore(ROOT_STORE).delete(path);
                            deleteRequest.onsuccess = () => {
                                deleteResult = path;
                            };
                            deleteRequest.onerror = () => {
                                deleteResult = new mobiletto_common_1.MobilettoError(`remove(${path}) deleteRequest error`);
                            };
                        }
                    };
                });
            });
            const toDelete = yield this._list(path, recursive, deleteVisitor);
            if (toDelete.length === 0) {
                if (quiet)
                    return [];
                throw new mobiletto_common_1.MobilettoNotFoundError(path);
            }
            if (deleteErrors.length === 0) {
                return deletedFiles.length === 0 ? [] : deletedFiles.length === 1 ? deletedFiles[0] : deletedFiles;
            }
            else {
                throw deleteErrors[0];
            }
        });
    }
}
exports.StorageClient = StorageClient;
const storageClient = (key, secret, opts) => {
    if (!key) {
        throw new mobiletto_common_1.MobilettoError("indexeddb.storageClient: key is required");
    }
    if (!opts || !opts.indexedDB) {
        throw new mobiletto_common_1.MobilettoError("indexeddb.storageClient: opts.indexedDB is required");
    }
    return new StorageClient(key, opts);
};
exports.storageClient = storageClient;
