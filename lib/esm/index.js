var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
import { dirname, basename } from "path";
import { logger, M_FILE, M_DIR, M_LINK, M_SPECIAL, isReadable, readStream, writeStream, closeStream, MobilettoError, MobilettoNotFoundError, } from "mobiletto-base";
import * as fs from "fs";
const DEFAULT_FILE_MODE = "0600";
const DEFAULT_DIR_MODE = "0700";
/* eslint-disable @typescript-eslint/no-explicit-any */
const isNotExistError = (err) => (err.code && (err.code === "ENOENT" || err.code === "ENOTDIR")) || false;
/* eslint-enable @typescript-eslint/no-explicit-any */
const fileType = (stat) => {
    if (stat.isDirectory()) {
        return M_DIR;
    }
    if (stat.isFile()) {
        return M_FILE;
    }
    if (stat.isSymbolicLink()) {
        return M_LINK;
    }
    return M_SPECIAL;
};
class StorageClient {
    constructor(baseDir, opts) {
        this.testConfig = () => __awaiter(this, void 0, void 0, function* () { return yield this.list(); });
        this.normalizePath = (path) => path.startsWith(this.baseDir) ? path : this.baseDir + (path.startsWith("/") ? path.substring(1) : path);
        this.denormalizePath = (path) => {
            const norm = path.startsWith(this.baseDir) ? path.substring(this.baseDir.length) : path;
            return norm.startsWith("/") ? norm.substring(1) : norm;
        };
        /* eslint-disable @typescript-eslint/no-explicit-any */
        this.ioError = (err, path, method) => 
        /* eslint-enable @typescript-eslint/no-explicit-any */
        isNotExistError(err)
            ? err instanceof MobilettoNotFoundError
                ? err
                : new MobilettoNotFoundError(this.denormalizePath(path))
            : err instanceof MobilettoError || err instanceof MobilettoNotFoundError
                ? err
                : new MobilettoError(`${method}(${path}) error: ${err}`, err);
        this.fileToObject = (dir) => (f) => {
            const normPath = (dir.endsWith("/") ? dir : dir + "/") + f;
            const stat = fs.lstatSync(normPath);
            const type = fileType(stat);
            const entry = {
                name: this.denormalizePath(normPath),
                type,
            };
            if (type === M_LINK) {
                const resolved = this.resolveSymlinks(normPath);
                entry.link = resolved.path;
            }
            return entry;
        };
        this.readDirFiles = (dir, recursive, visitor) => __awaiter(this, void 0, void 0, function* () {
            if (!recursive) {
                return yield this.dirFiles(dir, visitor);
            }
            const files = yield this.dirFiles(dir, visitor);
            for (const file of files) {
                if (file.type === M_DIR) {
                    yield this.readDirFiles(file.name, true, visitor);
                }
            }
        });
        this.dirFiles = (dir, visitor) => __awaiter(this, void 0, void 0, function* () {
            const norm = this.normalizePath(dir);
            try {
                const names = fs.readdirSync(norm);
                const files = names.map(this.fileToObject(norm));
                for (const f of files) {
                    yield visitor(f);
                }
                return files;
            }
            catch (e) {
                if (isNotExistError(e)) {
                    // try to list the parent directory and filter for just this file
                    const files = [];
                    try {
                        const parent = dirname(norm);
                        const base = basename(norm);
                        const names = fs.readdirSync(parent);
                        for (const n of names) {
                            if (n === base) {
                                const f = this.fileToObject(parent)(base);
                                files.push(f);
                                yield visitor(f);
                            }
                        }
                    }
                    catch (e2) {
                        logger.warn(`dirFiles (try-file) error: ${JSON.stringify(e2)}`);
                        throw this.ioError(e2, norm, "dirFiles");
                    }
                    if (files.length > 0) {
                        return files;
                    }
                    else {
                        logger.warn(`dirFiles (try-file) not found: ${norm}`);
                        throw this.ioError(e, norm, "dirFiles");
                    }
                }
                logger.warn(`dirFiles error: ${JSON.stringify(e)}`);
                throw this.ioError(e, norm, "dirFiles");
            }
        });
        if (!baseDir) {
            throw new MobilettoError("local.StorageClient: key (baseDir) is required");
        }
        this.fileMode = opts && opts.fileMode ? opts.fileMode : DEFAULT_FILE_MODE;
        this.dirMode = opts && opts.dirMode ? opts.dirMode : DEFAULT_DIR_MODE;
        let dir;
        if (!fs.existsSync(baseDir)) {
            if (opts === null || opts === void 0 ? void 0 : opts.createIfNotExist) {
                fs.mkdirSync(baseDir, { mode: this.dirMode, recursive: true });
                dir = baseDir;
            }
            else {
                throw new MobilettoError(`local.StorageClient: baseDir does not exist (and opts.createIfNotExist was not true): ${baseDir}`);
            }
        }
        else {
            const resolved = this.resolveSymlinks(baseDir);
            if (!resolved.stat.isDirectory()) {
                const dest = resolved.path === baseDir ? null : resolved.path;
                throw new MobilettoError(`local.StorageClient: baseDir is not a directory: ${baseDir}${dest ? ` (resolved to ${dest})` : ""}`);
            }
            dir = resolved.path;
        }
        this.baseDir = dir.endsWith("/") ? dir : dir + "/";
    }
    resolveSymlinks(path) {
        let stat = fs.lstatSync(path);
        let symlinksFollowed = false;
        while (!stat.isDirectory() && stat.isSymbolicLink()) {
            path = fs.readlinkSync(path);
            if (path.startsWith("private/")) {
                path = "/" + path;
            }
            stat = fs.lstatSync(path);
            symlinksFollowed = true;
        }
        return { path, stat, symlinksFollowed };
    }
    list(pth = "", recursive = false, visitor = null) {
        return __awaiter(this, void 0, void 0, function* () {
            const dir = this.normalizePath(pth);
            try {
                if (visitor === null) {
                    const results = [];
                    yield this.readDirFiles(dir, recursive, (obj) => __awaiter(this, void 0, void 0, function* () { return results.push(obj); }));
                    return results;
                }
                else {
                    return yield this.readDirFiles(dir, recursive, visitor);
                }
            }
            catch (err) {
                if (err instanceof MobilettoNotFoundError && !recursive && pth.includes("/")) {
                    // are we trying to list a single file?
                    const parentDir = this.normalizePath(dirname(pth));
                    const results = [];
                    yield this.readDirFiles(parentDir, false, (obj) => __awaiter(this, void 0, void 0, function* () {
                        if (obj.name === pth) {
                            results.push(obj);
                        }
                    }));
                    if (results.length === 0) {
                        throw err;
                    }
                    return results;
                }
                throw this.ioError(err, pth, "list");
            }
        });
    }
    metadata(path) {
        return __awaiter(this, void 0, void 0, function* () {
            const file = this.normalizePath(path);
            let lstat;
            try {
                lstat = fs.lstatSync(file);
            }
            catch (err) {
                throw this.ioError(err, path, "metadata");
            }
            if (!lstat) {
                throw new MobilettoError("metadata: lstat error");
            }
            const type = fileType(lstat);
            if (type === M_DIR && path !== "") {
                let contents;
                try {
                    contents = yield this.list(path);
                }
                catch (err) {
                    throw this.ioError(err, path, "metadata");
                }
                if (!contents || contents.length === 0) {
                    throw new MobilettoNotFoundError(path);
                }
            }
            return {
                name: this.denormalizePath(file),
                type,
                size: lstat.size,
                mtime: lstat.mtimeMs,
            };
        });
    }
    mkdirs(path) {
        try {
            logger.debug(`mkdirs: creating directory: ${path}`);
            fs.mkdirSync(path, { recursive: true, mode: this.dirMode });
        }
        catch (err) {
            throw new MobilettoError(`mkdirs: error creating directory ${path}: ${err}`, err);
        }
    }
    normalizePathAndEnsureParentDirs(path) {
        return __awaiter(this, void 0, void 0, function* () {
            const file = this.normalizePath(path);
            logger.debug(`read: reading path: ${path} - ${file}`);
            const parent = dirname(file);
            let dirStat;
            try {
                dirStat = fs.lstatSync(parent);
            }
            catch (err) {
                if (isNotExistError(err)) {
                    this.mkdirs(parent);
                }
                else {
                    throw new MobilettoError(`write: lstat error on ${parent}: ${err}`, err);
                }
            }
            if (typeof dirStat === "undefined") {
                this.mkdirs(parent);
            }
            else if (!dirStat.isDirectory()) {
                throw new MobilettoError(`write: not a directory: ${parent} (cannot write file ${file})`);
            }
            return file;
        });
    }
    write(path, generatorOrReadableStream) {
        return __awaiter(this, void 0, void 0, function* () {
            const file = yield this.normalizePathAndEnsureParentDirs(path);
            logger.debug(`write: writing path ${path} -> ${file}`);
            const stream = fs.createWriteStream(file, { mode: parseInt(this.fileMode, 8) });
            const writer = writeStream(stream);
            const closer = closeStream(stream);
            let count = 0;
            if (isReadable(generatorOrReadableStream)) {
                const readable = generatorOrReadableStream;
                /* eslint-disable @typescript-eslint/no-explicit-any */
                const streamHandler = (stream) => 
                /* eslint-enable @typescript-eslint/no-explicit-any */
                new Promise((resolve, reject) => {
                    stream.on("data", (data) => {
                        if (data) {
                            writer(data);
                            count += data.length;
                        }
                    });
                    stream.on("error", reject);
                    stream.on("end", () => {
                        closer();
                        resolve();
                    });
                });
                yield streamHandler(readable);
                return count;
            }
            const generator = generatorOrReadableStream;
            let chunk = (yield generator.next()).value;
            let nullCount = 0;
            while (chunk || nullCount < 5) {
                if (chunk) {
                    count += chunk.length;
                    writer(chunk);
                }
                else {
                    nullCount++;
                }
                chunk = (yield generator.next()).value;
            }
            closer();
            return count;
        });
    }
    read(path, callback, endCallback) {
        return __awaiter(this, void 0, void 0, function* () {
            const file = this.normalizePath(path);
            logger.debug(`read: reading path: ${path} - ${file}`);
            try {
                const stream = fs.createReadStream(file);
                return yield readStream(stream, callback, endCallback);
            }
            catch (err) {
                throw this.ioError(err, path, "read");
            }
        });
    }
    remove(path, optsOrRecursive, quiet) {
        return __awaiter(this, void 0, void 0, function* () {
            const file = this.normalizePath(path);
            const recursive = optsOrRecursive === true || (optsOrRecursive && optsOrRecursive.recursive);
            quiet || (quiet = typeof quiet === "undefined" && typeof optsOrRecursive === "object" && optsOrRecursive.quiet);
            logger.debug(`remove: deleting path: ${path} = ${file}`);
            try {
                fs.rmSync(file, { recursive: recursive, force: quiet, maxRetries: 2 });
            }
            catch (err) {
                if (isNotExistError(err)) {
                    if (!quiet) {
                        throw new MobilettoNotFoundError(path);
                    }
                }
                else {
                    throw new MobilettoError(`remove(${path}) error: ${err}`, err);
                }
            }
            return Promise.resolve(path);
        });
    }
}
export const storageClient = (key, secret, opts) => {
    if (!key) {
        throw new MobilettoError("local.storageClient: key is required");
    }
    return new StorageClient(key, opts);
};
