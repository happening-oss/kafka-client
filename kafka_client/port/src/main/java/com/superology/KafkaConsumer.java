package com.superology;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import java.math.BigInteger;

public class KafkaConsumer {
  public static void main(String[] args) {
    try (DataInputStream input = new DataInputStream(new FileInputStream("/dev/fd/3"))) {
      while (true) {
        int length = readInt(input);
        readBytes(input, length);
      }
    } catch (java.io.EOFException e) {
      return;
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static int readInt(DataInputStream input)
      throws IOException {
    byte[] bytes = readBytes(input, 4);
    return new BigInteger(bytes).intValue();
  }

  private static byte[] readBytes(DataInputStream input, int length)
      throws IOException {
    byte[] bytes = new byte[length];
    input.readFully(bytes);
    return bytes;
  }
}
