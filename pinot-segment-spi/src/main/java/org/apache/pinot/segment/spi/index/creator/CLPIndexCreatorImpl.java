package org.apache.pinot.segment.spi.index.creator;

import java.io.IOException;


public class CLPIndexCreatorImpl implements CLPIndexCreator {
  // Add a new CLP encoded string to the index.
  // 1. Add the string the dictionary of the column.
  @Override
  public void add(String value) {

  }

  @Override
  public void seal()
      throws IOException {

  }
}
