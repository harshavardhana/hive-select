package io.minio.hive.selectInputFromat;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import io.minio.hive.selectInputFromat.selectRecordReader;;

/**
 * A select input format for dealing with filtering records
 */

class SelectTextInputFormat extends TextInputFormat {

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf job, Reporter reporter)
        throws IOException {
        return new selectRecordReader((FileSplit) inputSplit, job);
    }
}
