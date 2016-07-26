package com.bimodeler.xlhive;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;

public class ExcelRecordReader implements RecordReader<LongWritable, Text> {

  private long row = 0;
  private long lastRow = 1;
  private InputStream is;
  private Iterator<Row> xlsRows;

  public ExcelRecordReader(InputSplit genericSplit, JobConf configuration) throws IOException, InvalidFormatException {
    FileSplit split = (FileSplit) genericSplit;
    final Path file = split.getPath();
    FileSystem fs = file.getFileSystem(configuration);
    is = fs.open(split.getPath());
    Workbook workbook = new WorkbookFactory().create(is);
    Sheet sheet = workbook.getSheetAt(0);
    lastRow = sheet.getLastRowNum();
    xlsRows = sheet.iterator();
  }

  @Override
  public float getProgress() throws IOException {
    return ((float) row) / (lastRow > 0 ? lastRow : 1);
  }

  @Override
  public void close() throws IOException {
    if (is != null) {
      is.close();
    }
  }

  @Override
  public boolean next(LongWritable k, Text v) throws IOException {
    if (xlsRows.hasNext()) {
      Row row = xlsRows.next();
      k.set(this.row);
      StringBuffer val = new StringBuffer();
      Iterator<Cell> cells = row.cellIterator();
      while (cells.hasNext()) {
        Cell cell = cells.next();
        switch (cell.getCellType()) {
          case Cell.CELL_TYPE_BLANK:
          case Cell.CELL_TYPE_FORMULA:
          case Cell.CELL_TYPE_ERROR:
            val.append("\\N").append(",");
            break;
          case Cell.CELL_TYPE_BOOLEAN:
            val.append(cell.getBooleanCellValue()).append(",");
            break;
          case Cell.CELL_TYPE_NUMERIC:
            val.append(cell.getNumericCellValue()).append(",");
            break;
          default: // Cell.CELL_TYPE_STRING
            val.append(cell.getStringCellValue().replace("\\","\\\\").replace(",", "\\,")).append(",");
            break;
        }
      }
      val.append("\n");
      v.set(val.toString());
      this.row++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public Text createValue() {
    return new Text();
  }

  @Override
  public long getPos() throws IOException {
    return row;
  }
}