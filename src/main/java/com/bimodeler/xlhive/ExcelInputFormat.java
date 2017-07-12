package com.bimodeler.xlhive;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

public class ExcelInputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit is, JobConf jc, Reporter rprtr) throws IOException {
        try {
            rprtr.setStatus(is.toString());
            return new ExcelRecordReader(is, jc);
        } catch (InvalidFormatException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        // Ensure mappers only process individualfiles (grid files should be read fully before being transformed)
        return false;
    }
}
