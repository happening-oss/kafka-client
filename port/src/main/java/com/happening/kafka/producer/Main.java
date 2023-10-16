package com.happening.kafka.producer;

import com.ericsson.otp.erlang.OtpErlangObject;
import com.happening.kafka.Utils;
import com.happening.kafka.port.Driver;
import com.happening.kafka.port.Erlang;
import com.happening.kafka.port.Output;
import com.happening.kafka.port.Port;
import com.happening.kafka.port.Worker;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;

public class Main implements Port {
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        Driver.run(args, new Main());
    }

    private final Map<String, Handler> dispatchMap = Map.ofEntries(
            Map.entry("partitions_for", this::partitionsFor),
            Map.entry("metrics", this::metrics),
            Map.entry("stop", this::stop),
            Map.entry("send", this::send)
    );

    @Override
    public int run(Worker worker, Output output, Object[] args) throws Exception {
        @SuppressWarnings("unchecked")
        var props = Utils.toProperties((Map<Object, Object>) args[0]);

        try (var producer = new Producer(props)) {
            while (true) {
                var command = worker.take();
                var exitCode = this.dispatchMap.get(command.name()).handle(producer, command, output);
                if (exitCode != null) {
                    return exitCode;
                }
            }
        }
    }

    private Integer metrics(Producer producer, Port.Command command, Output output) throws InterruptedException {
        OtpErlangObject response;
        try {
            var map = Erlang.toMap(
                    producer.metrics(),
                    metricNameEntry -> {
                        OtpErlangObject value;
                        var metricsValue = metricNameEntry.getValue().metricValue();
                        if (metricsValue instanceof Double val) {
                            value = Double.isNaN(val) ? Erlang.nil() : Erlang.doubleValue(val);
                        } else if (metricsValue instanceof Long val) {
                            value = Erlang.longValue(val);
                        } else {
                            value = Erlang.binary(metricsValue.toString());
                        }
                        return Erlang.mapEntry(Erlang.binary(metricNameEntry.getKey().name()), value);
                    }
            );
            response = Erlang.ok(map);
        } catch (Exception e) {
            response = Erlang.error(Erlang.binary(Utils.getErrorMessage(e)));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer partitionsFor(
            Producer producer, Port.Command command, Output output
    ) throws InterruptedException, KafkaException {
        var topic = (String) command.args()[0];

        OtpErlangObject response;
        try {
            var list = Erlang.toList(
                    producer.partitionsFor(topic),
                    entry -> Erlang.integer(entry.partition())
            );
            response = Erlang.ok(list);
        } catch (Exception e) {
            response = Erlang.error(Erlang.binary(Utils.getErrorMessage(e)));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer stop(Producer producer, Port.Command command, Output output) {
        producer.flush();
        return 0;
    }

    private Integer send(Producer producer, Port.Command command, Output output) {
        @SuppressWarnings("unchecked")
        var record = (Map<String, Object>) command.args()[0];

        var headers = new LinkedList<Header>();

        @SuppressWarnings("unchecked")
        var recordHeaders = (Collection<Object[]>) record.get("headers");
        for (var header : recordHeaders) {
            headers.add(new Header() {
                @Override
                public String key() {
                    return (String) header[0];
                }

                @Override
                public byte[] value() {
                    return (byte[]) header[1];
                }
            });
        }

        Callback callback = null;
        var callbackId = (byte[]) command.args()[1];
        if (callbackId != null) {
            callback = (metadata, e) -> {
                OtpErlangObject payload;

                if (e != null) {
                    if (e instanceof TimeoutException) {
                        payload = Erlang.error(Erlang.atom("timeout"));
                    } else {
                        payload = Erlang.error(Erlang.binary(Utils.getErrorMessage(e)));
                    }
                } else {
                    payload = Erlang.ok(
                            Erlang.integer(metadata.partition()),
                            Erlang.longValue(metadata.offset()),
                            Erlang.longValue(metadata.timestamp())
                    );
                }
                try {
                    output.emit(Erlang.tuple(Erlang.atom("on_completion"), Erlang.binary(callbackId), payload));
                } catch (InterruptedException ie) {
                    throw new InterruptException(ie);
                }
            };
        }

        producer.send(
                new ProducerRecord<>(
                        (String) record.get("topic"),
                        (Integer) record.get("partition"),
                        (Long) record.get("timestamp"),
                        (byte[]) record.get("key"),
                        (byte[]) record.get("value"),
                        headers
                ),
                callback
        );

        return null;
    }

    @FunctionalInterface
    interface Handler {
        Integer handle(Producer producer, Port.Command command, Output output) throws Exception;
    }
}

final class Producer extends KafkaProducer<byte[], byte[]> {
    Producer(Properties properties) {
        super(properties);
    }
}
