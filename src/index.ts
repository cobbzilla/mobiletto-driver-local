import { dirname, basename } from "path";
import {
    logger,
    M_FILE,
    M_DIR,
    M_LINK,
    M_SPECIAL,
    isReadable,
    readStream,
    writeStream,
    closeStream,
    MobilettoError,
    MobilettoNotFoundError,
    MobilettoMetadata,
    MobilettoReadFunc,
    MobilettoRemoveOptions,
    MobilettoVisitor,
    MobilettoWriteSource,
    MobilettoOptions,
    MobilettoDriverInfo,
} from "mobiletto-base";

import * as fs from "fs";
import { Stats } from "fs";

const DEFAULT_FILE_MODE = "0600";
const DEFAULT_DIR_MODE = "0700";

/* eslint-disable @typescript-eslint/no-explicit-any */
const isNotExistError = (err: any): boolean => (err.code && (err.code === "ENOENT" || err.code === "ENOTDIR")) || false;
/* eslint-enable @typescript-eslint/no-explicit-any */

const fileType = (stat: Stats) => {
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

export type LocalDriverOptions = MobilettoOptions & {
    fileMode?: string;
    dirMode?: string;
    createIfNotExist?: boolean;
};

export type FsMetadata = MobilettoMetadata & {
    link?: string;
};

export const LocalInfo: MobilettoDriverInfo = {
    driver: "local",
    scope: "local",
};

class StorageClient {
    baseDir;
    fileMode;
    dirMode;
    constructor(baseDir: string, opts?: LocalDriverOptions) {
        if (!baseDir) {
            throw new MobilettoError("local.StorageClient: key (baseDir) is required");
        }
        this.fileMode = opts && opts.fileMode ? opts.fileMode : DEFAULT_FILE_MODE;
        this.dirMode = opts && opts.dirMode ? opts.dirMode : DEFAULT_DIR_MODE;

        let dir: string;
        if (!fs.existsSync(baseDir)) {
            if (opts?.createIfNotExist) {
                fs.mkdirSync(baseDir, { mode: this.dirMode, recursive: true });
                dir = baseDir;
            } else {
                throw new MobilettoError(
                    `local.StorageClient: baseDir does not exist (and opts.createIfNotExist was not true): ${baseDir}`
                );
            }
        } else {
            const resolved = this.resolveSymlinks(baseDir);
            if (!resolved.stat.isDirectory()) {
                const dest = resolved.path === baseDir ? null : resolved.path;
                throw new MobilettoError(
                    `local.StorageClient: baseDir is not a directory: ${baseDir}${dest ? ` (resolved to ${dest})` : ""}`
                );
            }
            dir = resolved.path;
        }
        this.baseDir = dir.endsWith("/") ? dir : dir + "/";
    }
    resolveSymlinks(path: string) {
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
    testConfig = async () => await this.list();

    info = () => LocalInfo;

    normalizePath = (path: string) =>
        path.startsWith(this.baseDir) ? path : this.baseDir + (path.startsWith("/") ? path.substring(1) : path);
    denormalizePath = (path: string) => {
        const norm = path.startsWith(this.baseDir) ? path.substring(this.baseDir.length) : path;
        return norm.startsWith("/") ? norm.substring(1) : norm;
    };
    /* eslint-disable @typescript-eslint/no-explicit-any */
    ioError = (err: any, path: string, method: string) =>
        /* eslint-enable @typescript-eslint/no-explicit-any */
        isNotExistError(err)
            ? err instanceof MobilettoNotFoundError
                ? err
                : new MobilettoNotFoundError(this.denormalizePath(path))
            : err instanceof MobilettoError || err instanceof MobilettoNotFoundError
            ? err
            : new MobilettoError(`${method}(${path}) error: ${err}`, err);

    fileToObject = (dir: string) => (f: string) => {
        const normPath = (dir.endsWith("/") ? dir : dir + "/") + f;
        const stat = fs.lstatSync(normPath);
        const type = fileType(stat);
        const entry: FsMetadata = {
            name: this.denormalizePath(normPath),
            type,
        };
        if (type === M_LINK) {
            const resolved = this.resolveSymlinks(normPath);
            entry.link = resolved.path;
        }
        return entry;
    };

    readDirFiles = async (dir: string, recursive: boolean, visitor: MobilettoVisitor) => {
        if (!recursive) {
            return await this.dirFiles(dir, visitor);
        }
        const files = await this.dirFiles(dir, visitor);
        for (const file of files) {
            if (file.type === M_DIR) {
                await this.readDirFiles(file.name, true, visitor);
            }
        }
    };

    dirFiles = async (dir: string, visitor: MobilettoVisitor) => {
        const norm = this.normalizePath(dir);
        try {
            const names: string[] = fs.readdirSync(norm);
            const files: FsMetadata[] = names.map(this.fileToObject(norm));
            for (const f of files) {
                await visitor(f);
            }
            return files;
        } catch (e) {
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
                            await visitor(f);
                        }
                    }
                } catch (e2) {
                    logger.warn(`dirFiles (try-file) error: ${JSON.stringify(e2)}`);
                    throw this.ioError(e2, norm, "dirFiles");
                }
                if (files.length > 0) {
                    return files;
                } else {
                    logger.warn(`dirFiles (try-file) not found: ${norm}`);
                    throw this.ioError(e, norm, "dirFiles");
                }
            }
            logger.warn(`dirFiles error: ${JSON.stringify(e)}`);
            throw this.ioError(e, norm, "dirFiles");
        }
    };

    async list(pth = "", recursive = false, visitor = null) {
        const dir = this.normalizePath(pth);
        try {
            if (visitor === null) {
                const results: MobilettoMetadata[] = [];
                await this.readDirFiles(dir, recursive, async (obj: MobilettoMetadata) => results.push(obj));
                return results;
            } else {
                return await this.readDirFiles(dir, recursive, visitor);
            }
        } catch (err) {
            if (err instanceof MobilettoNotFoundError && !recursive && pth.includes("/")) {
                // are we trying to list a single file?
                const parentDir = this.normalizePath(dirname(pth));
                const results: MobilettoMetadata[] = [];
                await this.readDirFiles(parentDir, false, async (obj: MobilettoMetadata) => {
                    if (obj.name === pth) {
                        results.push(obj);
                    }
                });
                if (results.length === 0) {
                    throw err;
                }
                return results;
            }
            throw this.ioError(err, pth, "list");
        }
    }
    async metadata(path: string): Promise<MobilettoMetadata> {
        const file = this.normalizePath(path);
        let lstat: fs.Stats;
        try {
            lstat = fs.lstatSync(file);
        } catch (err) {
            throw this.ioError(err, path, "metadata");
        }
        if (!lstat) {
            throw new MobilettoError("metadata: lstat error");
        }
        const type = fileType(lstat);
        if (type === M_DIR && path !== "") {
            let contents: MobilettoMetadata[] | undefined;
            try {
                contents = await this.list(path);
            } catch (err) {
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
    }

    mkdirs(path: string) {
        try {
            logger.debug(`mkdirs: creating directory: ${path}`);
            fs.mkdirSync(path, { recursive: true, mode: this.dirMode });
        } catch (err) {
            throw new MobilettoError(`mkdirs: error creating directory ${path}: ${err}`, err);
        }
    }

    async normalizePathAndEnsureParentDirs(path: string) {
        const file = this.normalizePath(path);

        logger.debug(`read: reading path: ${path} - ${file}`);
        const parent = dirname(file);
        let dirStat;
        try {
            dirStat = fs.lstatSync(parent);
        } catch (err) {
            if (isNotExistError(err)) {
                this.mkdirs(parent);
            } else {
                throw new MobilettoError(`write: lstat error on ${parent}: ${err}`, err);
            }
        }
        if (typeof dirStat === "undefined") {
            this.mkdirs(parent);
        } else if (!dirStat.isDirectory()) {
            throw new MobilettoError(`write: not a directory: ${parent} (cannot write file ${file})`);
        }
        return file;
    }

    async write(path: string, generatorOrReadableStream: MobilettoWriteSource) {
        const file = await this.normalizePathAndEnsureParentDirs(path);
        logger.debug(`write: writing path ${path} -> ${file}`);
        const stream = fs.createWriteStream(file, { mode: parseInt(this.fileMode, 8) });
        const writer = writeStream(stream);
        const closer = closeStream(stream);
        let count = 0;

        if (isReadable(generatorOrReadableStream)) {
            const readable = generatorOrReadableStream;
            /* eslint-disable @typescript-eslint/no-explicit-any */
            const streamHandler = (stream: any) =>
                /* eslint-enable @typescript-eslint/no-explicit-any */
                new Promise<void>((resolve, reject) => {
                    stream.on("data", (data: Buffer) => {
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
            await streamHandler(readable);
            return count;
        }

        const generator: MobilettoReadFunc = generatorOrReadableStream as MobilettoReadFunc;
        let chunk = (await generator.next()).value;
        let nullCount = 0;
        while (chunk || nullCount < 5) {
            if (chunk) {
                count += chunk.length;
                writer(chunk);
            } else {
                nullCount++;
            }
            chunk = (await generator.next()).value;
        }
        closer();
        return count;
    }

    async read(path: string, callback: (chunk: Buffer) => void, endCallback?: () => void): Promise<number> {
        const file = this.normalizePath(path);
        logger.debug(`read: reading path: ${path} - ${file}`);
        try {
            const stream = fs.createReadStream(file);
            return await readStream(stream, callback, endCallback);
        } catch (err) {
            throw this.ioError(err, path, "read");
        }
    }

    async remove(
        path: string,
        optsOrRecursive?: MobilettoRemoveOptions | boolean,
        quiet?: boolean
    ): Promise<string | string[]> {
        const file = this.normalizePath(path);
        const recursive = optsOrRecursive === true || (optsOrRecursive && optsOrRecursive.recursive);
        quiet ||= typeof quiet === "undefined" && typeof optsOrRecursive === "object" && optsOrRecursive.quiet;
        logger.debug(`remove: deleting path: ${path} = ${file}`);
        try {
            fs.rmSync(file, { recursive: recursive, force: quiet, maxRetries: 2 });
        } catch (err) {
            if (isNotExistError(err)) {
                if (!quiet) {
                    throw new MobilettoNotFoundError(path);
                }
            } else {
                throw new MobilettoError(`remove(${path}) error: ${err}`, err);
            }
        }
        return Promise.resolve(path);
    }
}

export const storageClient = (key: string, secret: string, opts: MobilettoOptions) => {
    if (!key) {
        throw new MobilettoError("local.storageClient: key is required");
    }
    return new StorageClient(key, opts);
};
