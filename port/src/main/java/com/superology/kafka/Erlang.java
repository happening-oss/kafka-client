package com.superology.kafka;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import com.ericsson.otp.erlang.*;

/*
 * Handles encoding and decoding of Erlang's external term format, which is used
 * to exchange messages between Elixir and Java.
 *
 * Most of the heavy lifting is powered by JInterface
 * (https://www.erlang.org/doc/apps/jinterface/java/com/ericsson/otp/erlang/package-summary.html).
 */
class Erlang {
  // This is basically a Java version of Erlang's term_to_binary.
  public static byte[] encode(OtpErlangObject erlangTerm) throws IOException {
    try (var otpOutStream = new OtpOutputStream(erlangTerm);
        var byteStream = new java.io.ByteArrayOutputStream()) {
      // OtpOutputStream.writeToAndFlush produces the binary without the leading
      // version number byte (131), so we need to include it ourselves.
      //
      // The version number is not included because JInterface is developed for
      // exchanging messages between Erlang nodes, and in this mode of operation the
      // leading version byte is not included.
      //
      // See https://www.erlang.org/doc/apps/erts/erl_ext_dist.html for details

      byteStream.write(131);
      otpOutStream.writeToAndFlush(byteStream);

      return byteStream.toByteArray();
    }
  }

  public static OtpErlangTuple tuple(OtpErlangObject... elements) {
    return new OtpErlangTuple(elements);
  }

  public static OtpErlangTuple ok(OtpErlangObject value) {
    return tuple(new OtpErlangAtom("ok"), value);
  }

  public static OtpErlangTuple error(OtpErlangObject value) {
    return tuple(new OtpErlangAtom("error"), value);
  }

  public static <T> OtpErlangList toList(Iterable<T> iterable, Function<T, OtpErlangObject> mapper) {
    var elements = StreamSupport.stream(iterable.spliterator(), false).map(mapper).toArray(OtpErlangObject[]::new);
    return new OtpErlangList(elements);
  }

  public static <K, V> OtpErlangMap toMap(
      Map<K, V> javaMap,
      Function<Map.Entry<K, V>, Map.Entry<OtpErlangObject, OtpErlangObject>> mapper) {
    var erlangMap = new OtpErlangMap();

    for (var entry : javaMap.entrySet()) {
      var erlangEntry = mapper.apply(entry);
      erlangMap.put(erlangEntry.getKey(), erlangEntry.getValue());
    }

    return erlangMap;
  }

  public static Map.Entry<OtpErlangObject, OtpErlangObject> mapEntry(OtpErlangObject key,
      OtpErlangObject value) {
    return new AbstractMap.SimpleEntry<>(key, value);
  }

  /*
   * Decodes an Erlang term format (produced via `:erlang.term_to_binary`) into
   * a hierarchy of Java objects, with the following rules:
   *
   * - integer number is decoded as an integer or a long, depending on its value
   * - true/false atoms are decoded as a boolean
   * - nil atom is decoded as null
   * - other atoms are decoded as a string
   * - binary is decoded as a string
   * - list is decoded as Collection<Object>
   * - tuple is decoded as Object[]
   * - map is decoded as Map<Object, Object>
   */
  public static Object decode(byte[] encoded) throws Exception {
    try (var inputStream = new OtpInputStream(encoded)) {
      return fromErlang(inputStream.read_any());
    }
  }

  private static Object fromErlang(OtpErlangObject value) throws Exception {
    if (value instanceof OtpErlangList)
      return Erlang.fromErlangList((OtpErlangList) value);
    else if (value instanceof OtpErlangTuple)
      return Erlang.fromErlangList(Arrays.asList(((OtpErlangTuple) value).elements())).toArray();
    else if (value instanceof OtpErlangMap)
      return Erlang.fromErlangMap((OtpErlangMap) value);
    else if (value instanceof OtpErlangBinary)
      return new String(((OtpErlangBinary) value).binaryValue());
    else if (value instanceof OtpErlangLong) {
      var number = ((OtpErlangLong) value).longValue();
      if (number >= Integer.MIN_VALUE && number <= Integer.MAX_VALUE)
        return (int) number;
      else
        return number;
    } else if (value instanceof OtpErlangAtom) {
      var atomValue = ((OtpErlangAtom) value).atomValue();
      switch (atomValue) {
        case "true":
        case "false":
          return Boolean.parseBoolean(atomValue);

        case "nil":
          return null;

        default:
          return atomValue;
      }
    }

    throw new Exception("error converting " + value.getClass() + " to java object: " + value);
  }

  private static Collection<Object> fromErlangList(Iterable<OtpErlangObject> objects) throws Exception {
    var result = new ArrayList<Object>();

    for (var object : objects)
      result.add(fromErlang(object));

    return result;
  }

  private static Map<Object, Object> fromErlangMap(OtpErlangMap otpMap) throws Exception {
    var map = new HashMap<Object, Object>();

    for (var param : otpMap.entrySet()) {
      var key = fromErlang(param.getKey());
      var value = fromErlang(param.getValue());
      map.put(key, value);
    }

    return map;
  }
}
