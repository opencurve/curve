/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Curve
 * Created Date: 2023-08-01
 * Author: NetEase Media Bigdata
 */

package io.opencurve.curve.fs.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import io.opencurve.curve.fs.libfs.CurveFSMount;
import io.opencurve.curve.fs.common.StackLogger;

import java.io.IOException;

/**
 * <p>
 * An {@link FSInputStream} for a CurveFileSystem and corresponding
 * Curve instance.
 */
public class CurveFSInputStream extends FSInputStream {
    private static final Log LOG = LogFactory.getLog(CurveFSInputStream.class);
    private static final StackLogger logger = new StackLogger("CurveFSInputStream", 0);
    private boolean closed;

    private int fileHandle;

    private long fileLength;

    private CurveFSProto curve;

    private byte[] buffer;
    private int bufPos = 0;
    private int bufValid = 0;
    private long curvePos = 0;

    /**
     * Create a new CurveInputStream.
     * @param conf The system configuration. Unused.
     * @param fh The filehandle provided by Curve to reference.
     * @param flength The current length of the file. If the length changes
     * you will need to close and re-open it to access the new data.
     */
    public CurveFSInputStream(Configuration conf, CurveFSProto curvefs,
                              int fh, long flength, int bufferSize) {
        logger.log("CurveFSInputStream");

        // Whoever's calling the constructor is responsible for doing the actual curve_open
        // call and providing the file handle.
        fileLength = flength;
        fileHandle = fh;
        closed = false;
        curve = curvefs;
        buffer = new byte[1<<21];
        LOG.debug("CurveInputStream constructor: initializing stream with fh "
                  + fh + " and file length " + flength);
    }

    /** Curve likes things to be closed before it shuts down,
     * so closing the IOStream stuff voluntarily in a finalizer is good
     */
    protected void finalize() throws Throwable {
        logger.log("finalize");

        try {
            if (!closed) {
                close();
            }
        } finally {
            super.finalize();
        }
    }

    private synchronized boolean fillBuffer() throws IOException {
        logger.log("fillBuffer");

        bufValid = curve.read(fileHandle, buffer, buffer.length, -1);
        bufPos = 0;
        if (bufValid < 0) {
            int err = bufValid;

            bufValid = 0;

            // attempt to reset to old position. If it fails, too bad.
            curve.lseek(fileHandle, curvePos, CurveFSMount.SEEK_SET);
            throw new IOException("Failed to fill read buffer! Error code:" + err);
        }
        curvePos += bufValid;
        return (bufValid != 0);
    }

    /*
     * Get the current position of the stream.
     */
    public synchronized long getPos() throws IOException {
        return curvePos - bufValid + bufPos;
    }

    /**
     * Find the number of bytes remaining in the file.
     */
    @Override
    public synchronized int available() throws IOException {
        if (closed) {
            throw new IOException("file is closed");
        }
        return (int) (fileLength - getPos());
    }

    public synchronized void seek(long targetPos) throws IOException {
        logger.log("seek", targetPos);

        LOG.trace("CurveInputStream.seek: Seeking to position " + targetPos + " on fd "
                  + fileHandle);
        if (targetPos > fileLength) {
            throw new IOException("CurveInputStream.seek: failed seek to position " +
                                  targetPos + " on fd " + fileHandle +
                                  ": Cannot seek after EOF " + fileLength);
        }
        long oldPos = curvePos;

        curvePos = curve.lseek(fileHandle, targetPos, CurveFSMount.SEEK_SET);
        bufValid = 0;
        bufPos = 0;
        if (curvePos < 0) {
            curvePos = oldPos;
            throw new IOException("Curve failed to seek to new position!");
        }
    }

    /**
     * Failovers are handled by the Curve code at a very low level;
     * if there are issues that can be solved by changing sources
     * they'll be dealt with before anybody even tries to call this method!
     * @return false.
     */
    public synchronized boolean seekToNewSource(long targetPos) {
        logger.log("seekToNewSource", targetPos);
        return false;
    }

    /**
     * Read a byte from the file.
     * @return the next byte.
     */
    @Override
    public synchronized int read() throws IOException {
        logger.log("read");
        LOG.trace(
                  "CurveInputStream.read: Reading a single byte from fd " + fileHandle
                  + " by calling general read function");

        byte result[] = new byte[1];

        if (getPos() >= fileLength) {
            return -1;
        }

        if (-1 == read(result, 0, 1)) {
            return -1;
        }

        if (result[0] < 0) {
            return 256 + (int) result[0];
        } else {
            return result[0];
        }
    }

    /**
     * Read a specified number of bytes from the file into a byte[].
     * @param buf the byte array to read into.
     * @param off the offset to start at in the file
     * @param len the number of bytes to read
     * @return 0 if successful, otherwise an error code.
     * @throws IOException on bad input.
     */
    @Override
    public synchronized int read(byte buf[], int off, int len) throws IOException {
        logger.log("read", off, len);

        LOG.trace(
            "CurveInputStream.read: Reading " + len + " bytes from fd " + fileHandle);

        if (closed) {
            throw new IOException(
                "CurveInputStream.read: cannot read " + len + " bytes from fd "
                + fileHandle + ": stream closed");
        }

        // ensure we're not past the end of the file
        if (getPos() >= fileLength) {
            LOG.debug(
                "CurveInputStream.read: cannot read " + len + " bytes from fd "
                + fileHandle + ": current position is " + getPos()
                + " and file length is " + fileLength);

            return -1;
        }

        int totalRead = 0;
        int initialLen = len;
        int read;

        do {
            read = Math.min(len, bufValid - bufPos);
            try {
                System.arraycopy(buffer, bufPos, buf, off, read);
            } catch (IndexOutOfBoundsException ie) {
                throw new IOException(
                    "CurveInputStream.read: Indices out of bounds:" + "read length is "
                    + len + ", buffer offset is " + off + ", and buffer size is "
                    + buf.length);
            } catch (ArrayStoreException ae) {
                throw new IOException(
                    "Uh-oh, CurveInputStream failed to do an array"
                    + "copy due to type mismatch...");
            } catch (NullPointerException ne) {
                throw new IOException(
                    "CurveInputStream.read: cannot read " + len + "bytes from fd:"
                    + fileHandle + ": buf is null");
            }
            bufPos += read;
            len -= read;
            off += read;
            totalRead += read;
        } while (len > 0 && fillBuffer());

        LOG.trace(
            "CurveInputStream.read: Reading " + initialLen + " bytes from fd "
            + fileHandle + ": succeeded in reading " + totalRead + " bytes");
        return totalRead;
    }

    /**
     * Close the CurveInputStream and release the associated filehandle.
     */
    @Override
    public void close() throws IOException {
        logger.log("close");

        LOG.trace("CurveOutputStream.close:enter");
        if (!closed) {
            curve.close(fileHandle);
            closed = true;
            LOG.trace("CurveOutputStream.close:exit");
        }
    }
}
