const { Buffer } = require('buffer')

const {
    M_FILE, M_DIR, M_LINK, M_SPECIAL, isAsyncGenerator, isReadable, logger,
    MobilettoError, MobilettoNotFoundError, readStream, writeStream, closeStream
} = require('mobiletto-common')

REQUIRE = require
IDB_SCHEMA_VERSION = 1

const ROOT_STORE = 'rootStore'

class StorageClient {
    indexedDB
    dbPromise
    db = null
    rootStore = null
    constructor(dbName, opts) {
        if (!dbName) {
            throw new MobilettoError('indexeddb.StorageClient: key (dbName) is required')
        }
        if (opts && opts.indexedDB) {
            this.indexedDB = opts.indexedDB
        } else {
            throw new MobilettoError('indexeddb.StorageClient: opts.indexedDB is required')
        }
        this.dbPromise = new Promise((resolve, reject) => {
            const openOrCreateDB = this.indexedDB.open(dbName, IDB_SCHEMA_VERSION)
            openOrCreateDB.onerror = (event) => {
                reject(`indexedDB.open failed: ${event.target.errorCode}`)
            }
            openOrCreateDB.onsuccess = () => {
                resolve(this.db = openOrCreateDB.result)
            }

            openOrCreateDB.onupgradeneeded = (event) => {
                if (this.db != null && this.db !== event.target.result) {
                    reject(new MobilettoError(`indexedDB.upgradeneeded: event.target.result !== this.db`))
                    return
                }
                const db = event.target.result
                db.onerror = (event) => {
                    reject(new MobilettoError(`indexedDB.open failed (upgradeneeded): ${event.target.result}`))
                }

                this.rootStore = db.createObjectStore(ROOT_STORE, {})
            }
        })
    }

    testConfig = async () => await this.list()

    mdb = async () => {
        if (this.db) return this.db
        const db = await this.dbPromise
        if (db) return db
        if (this.db) return this.db
        throw new MobilettoError(`mdb: error getting database`)
    }

    async list (pth = '', recursive = false, visitor = null) {
        return this._list(pth, recursive, visitor)
    }

    async _list (pth = '', recursive = false, visitor = null) {
        const db = await this.mdb()
        return new Promise(async (resolve, reject) => {
            try {
                const listTx = db.transaction(ROOT_STORE, "readonly")
                listTx.oncomplete = (event) => {
                    logger.info(`list(${pth}) listTx completed`)
                }
                listTx.onerror = (event) => {
                    logger.error(`list(${pth}) listTx error`)
                }

                const listRequest = listTx.objectStore(ROOT_STORE).getAll()
                listRequest.onerror = (event) => {
                    reject(event)
                }
                listRequest.onsuccess = async (event) => {
                    let foundExact = null
                    const filtered = listRequest.result
                        .sort((o1, o2) => o1.name.localeCompare(o2.name))
                        .map(o => {
                            const prefixMatch = o.name.startsWith(pth)
                            if (o.name === pth) {
                                foundExact = o
                                return o
                            }
                            if (prefixMatch) {
                                if (recursive) return o
                                const normMatch = pth === '' ? '' : pth.endsWith('/') ? pth : pth + '/'
                                if (o.name.startsWith(normMatch) && o.name.length > normMatch.length) {
                                    const nextSlash = o.name.indexOf('/', normMatch.length + 1)
                                    if (nextSlash === -1) return o
                                    const dir = {
                                        type: M_DIR,
                                        name: o.name.substring(0, nextSlash)
                                    }
                                    return dir
                                }
                            }
                            return null
                        })
                        .filter(o => o != null)
                        .filter((value, index, self) =>
                            value != null && index === self.findIndex(t => t.name === value.name)
                        )
                    if (foundExact && !recursive) {
                        if (visitor) await visitor(foundExact)
                        resolve([foundExact])
                    } else {
                        if (visitor) {
                            for (const f of filtered.filter(o => o.type === M_FILE)) {
                                await visitor(f)
                            }
                        }
                        resolve(filtered)
                    }
                }
            } catch (e) {
                logger.error(`list(${pth}): error ${e}`)
            }
        })
    }

    async metadata (path) {
        const db = await this.mdb()
        let data = null
        return new Promise((resolve, reject) => {
            const readMetaTx = db.transaction(ROOT_STORE, "readonly")
            readMetaTx.oncomplete = (event) => {
                if (!data) {
                    reject(new MobilettoNotFoundError(path))
                    return
                }
                try {
                    delete data.bytes
                    resolve(data)
                } catch (e) {
                    reject(new MobilettoError(`metadata(${path}) error parsing data: ${e}`))
                }
            }
            readMetaTx.onerror = (event) => {
                reject(new MobilettoError(`metadata(${path}) readMetaTx error`))
            }
            const readRequest = readMetaTx.objectStore(ROOT_STORE).get(path)
            readRequest.onerror = (event) => {
                reject(new MobilettoError(`metadata(${path}) readRequest error`))
            }
            readRequest.onsuccess = (event) => {
                data = readRequest.result
            }
        })
    }

    async write (path, generatorOrReadableStream) {
        let bytes = []
        const db = await this.mdb()
        return new Promise(async (resolve, reject) => {
            if (isReadable(generatorOrReadableStream)) {
                const readable = generatorOrReadableStream
                const streamHandler = stream =>
                    new Promise((resolve2, reject2) => {
                        stream.on('data', (data) => {
                            if (data) {
                                bytes.push(...data)
                            }
                        })
                        stream.on('error', (e) => {
                            reject2(e)
                        })
                        stream.on('end', () => {
                            resolve2()
                        })
                    })
                await streamHandler(readable)
            } else {
                const generator = generatorOrReadableStream
                let chunk = isAsyncGenerator(generator)
                    ? (await generator.next()).value
                    : generator.next().value
                let nullCount = 0
                while (chunk || nullCount < 5) {
                    if (chunk) {
                        bytes.push(...chunk)
                    } else {
                        nullCount++
                    }
                    chunk = isAsyncGenerator(generator)
                        ? (await generator.next()).value
                        : generator.next().value
                }
            }

            const writeTx = db.transaction(ROOT_STORE, "readwrite")
            writeTx.oncomplete = (event) => {
                logger.info(`write(${path}) writeTx complete`)
                resolve(bytes.length)
            }
            writeTx.onerror = (event) => {
                reject(new MobilettoError(`write(${path}) writeTx error`))
            }

            const writeObject = {
                name: path,
                type: M_FILE,
                bytes,
                size: bytes.length,
                mtime: Date.now()
            }
            const writeRequest = writeTx.objectStore(ROOT_STORE).put(writeObject, path)
            writeRequest.onerror = (event) => {
                reject(new MobilettoError(`write(${path}) writeRequest error`))
            }
            writeRequest.onsuccess = (event) => {
                logger.info(`write(${path}) writeRequest success`)
                resolve(bytes.length)
            }
        })
    }

    async read (path, callback, endCallback = null) {
        const db = await this.mdb()
        let bytesRead = null
        return new Promise((resolve, reject) => {
            const readTx = db.transaction(ROOT_STORE, 'readonly')
            let notFound = null
            readTx.oncomplete = (event) => {
                if (bytesRead) {
                    resolve(bytesRead)
                } else if (notFound) {
                    reject(notFound)
                } else {
                    logger.warn(`read(${path}) readTx complete but neither bytesRead nor notFound was set`)
                }
            }
            readTx.onerror = (event) => {
                reject(new MobilettoError(`read(${path}) readTx error`))
            }

            const readRequest = readTx.objectStore(ROOT_STORE).get(path)
            readRequest.onerror = (event) => {
                reject(new MobilettoError(`read(${path}) readRequest error`))
            }
            readRequest.onsuccess = (event) => {
                if (typeof (readRequest.result) === 'undefined') {
                    notFound = new MobilettoNotFoundError(path)
                    return
                }
                if (readRequest.result && readRequest.result.bytes && readRequest.result.bytes.length > 0) {
                    let bytes
                    if (typeof (readRequest.result.bytes[0]) === 'string') {
                        bytes = Buffer.from(readRequest.result.bytes.join(''))
                    } else {
                        bytes = Buffer.from(readRequest.result.bytes)
                    }
                    callback(bytes)
                    if (endCallback) endCallback()
                    bytesRead = bytes.length
                }
            }
        })
    }

    async remove (path, recursive, quiet) {
        const db = await this.mdb()
        const deleteErrors = []
        const deletedFiles = []
        const deleteVisitor = async file => await new Promise(async (resolve, reject) => {
            const path = file.name
            if (file.type !== M_FILE) {
                logger.warn(`remove(${path}): not a file`)
                if (quiet) {
                    resolve(true)
                } else {
                    const err = new MobilettoError(`remove(${path}): not a file`);
                    deleteErrors.push(err)
                    reject(err)
                }
            }
            let deleteResult = null
            const deleteTx = db.transaction(ROOT_STORE, 'readwrite')
            deleteTx.oncomplete = (event) => {
                if (deleteResult == null) {
                    if (quiet) {
                        logger.warn(`remove(${path}): deleteResult was null`)
                        resolve(true)
                    } else {
                        const err = new MobilettoError(`remove(${path}): deleteResult was null`);
                        deleteErrors.push(err)
                        reject(err)
                    }
                } else {
                    if (deleteResult instanceof MobilettoNotFoundError || deleteResult instanceof MobilettoError) {
                        if (quiet) {
                            resolve(true)
                        } else {
                            deleteErrors.push(deleteResult)
                            reject(deleteResult)
                        }
                    } else {
                        deletedFiles.push(deleteResult)
                        resolve(deleteResult)
                    }
                }
            }
            deleteTx.onerror = (event) => {
                if (quiet) {
                    logger.warn(`remove(${path}): deleteTx error`)
                    resolve(true)
                } else {
                    const err = new MobilettoError(`remove(${path}) deleteTx error`);
                    deleteErrors.push(err)
                    reject(err)
                }
            }

            const findRequest = deleteTx.objectStore(ROOT_STORE).get(path)
            findRequest.onerror = (event) => {
                logger.warn(`remove(${path}): error`)
                deleteResult = new MobilettoNotFoundError(path)
            }
            findRequest.onsuccess = (event) => {
                if (typeof (findRequest.result) === 'undefined') {
                    deleteResult = new MobilettoNotFoundError(path)
                } else {
                    const deleteRequest = deleteTx.objectStore(ROOT_STORE).delete(path)
                    deleteRequest.onsuccess = (event) => {
                        deleteResult = path
                    }
                    deleteRequest.onerror = (event) => {
                        deleteResult = new MobilettoError(`remove(${path}) deleteRequest error`)
                    }
                }
            }
        })

        const toDelete = await this._list(path, recursive, deleteVisitor)
        if (toDelete.length === 0) {
            if (quiet) return true
            throw new MobilettoNotFoundError(path)
        }

        if (deleteErrors.length === 0) {
            return deletedFiles.length === 0
                ? quiet
                : deletedFiles.length === 1 ? deletedFiles[0] : deletedFiles
        } else {
            throw deleteErrors[0]
        }
    }
}

function storageClient (key, secret, opts) {
    if (!key) {
        throw new MobilettoError('indexeddb.storageClient: key is required')
    }
    if (!opts || !opts.indexedDB) {
        throw new MobilettoError('indexeddb.storageClient: opts.indexedDB is required')
    }
    return new StorageClient(key, opts)
}

module.exports = { storageClient }
