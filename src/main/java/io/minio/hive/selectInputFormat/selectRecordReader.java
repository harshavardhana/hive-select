package io.minio.hive.selectInputFromat;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

public class selectRecordReader implements RecordReader<LongWritable, Text> {

    private static final Log LOG = LogFactory.getLog(selectRecordReader.class.getName());
    private static final int NUMBER_OF_FIELDS = 5;

    // Variables to hold positions of the fields
    private static final int POS_UID = 3;

    // Variables to hold length of the fields
    private static final int LEN_UID = 8;

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    int maxLineLength;

    public selectRecordReader(FileSplit inputSplit, Configuration job) throws IOException {
        maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        start = inputSplit.getStart();
        end = start + inputSplit.getLength();
        final Path file = inputSplit.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // Open file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(file);
        boolean skipFirstLine = false;
        if (codec != null) {
            in = new LineReader(codec.createInputStream(fileIn), job);
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            in = new LineReader(fileIn, job);
        }

        if (skipFirstLine) {
            start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    public selectRecordReader(InputStream in, long offset, long endOffset, int maxLineLength) {
        this.maxLineLength = maxLineLength;
        this.in = new LineReader(in);
        this.start = offset;
        this.pos = offset;
        this.end = endOffset;
    }

    public selectRecordReader(InputStream in, long offset, long endOffset, Configuration job) throws IOException {
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        this.in = new LineReader(in, job);
        this.start = offset;
        this.pos = offset;
        this.end = endOffset;
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public Text createValue() {
        return new Text();
    }

    /**
     * Reads the next record in the split. Records whose fields don't satisfy
     * the length condition are skipped
     *
     * @param key
     *            key of the record which will map to the byte offset of the
     *            record's line
     * @param value
     *            the record in text format
     * @return true if a record existed, false otherwise
     * @throws IOException
     */
    public synchronized boolean next(LongWritable key, Text value) throws IOException {
        // Stay within the split
        // Text val = new Text();
        while (pos < end) {
            LOG.warn("Reading");
            int newSize = in.readLine(value, maxLineLength,
                                      Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));

            if (newSize == 0)
                return false;
            String str = value.toString();
            String[] fields = str.split("\\|", -1);

            if (fields[POS_UID].toString().length() > LEN_UID) {

                LOG.warn("Skipping line at position " + (pos) + " with incorrect field length");
                pos += newSize;
                continue;
            } else {
                LOG.warn("pos: " + pos + "val: " + str);
                key.set(pos);
                value.set(str);
            }

            pos += newSize;

            if (newSize < maxLineLength && fields.length == NUMBER_OF_FIELDS)
                return true;

            if (fields.length != NUMBER_OF_FIELDS) {
                LOG.warn("Skipped line at position " + (pos - newSize) + " with incorrect number of fields (expected "
                         + NUMBER_OF_FIELDS + " but found " + fields.length + ")");
            } else {
                LOG.warn("Skipped line of size " + newSize + " at position " + (pos - newSize));
            }
        }
        return false;
    }

    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    public synchronized long getPos() throws IOException {
        return pos;
    }

    public synchronized void close() throws IOException {
        if (in != null)
            in.close();
    }

}
