package org.apache.pinot.segment.spi.index.creator;

import java.io.IOException;


public interface CLPIndexCreator {
  /**
   * Adds a new value to the CLP index.
   */
  void add(String value);

  /**
   * Seals the index and flushes it to disk.
   */
  void seal()
      throws IOException;
}
