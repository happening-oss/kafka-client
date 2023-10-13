package com.happening.kafka.port;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/*
 * The output thread of a port program. This thread is responsible for sending
 * messages to Elixir. Messages are sent as Erlang/Elixir terms.
 *
 * See {@link Driver} for details on port architecture. See {@link Erlang} for
 * details on data encoding.
 */
public final class Output implements Runnable {
    static Output start() {
        var output = new Output();

        // Using a daemon thread to ensure program termination if the main thread stops.
        var consumerThread = new Thread(output);
        consumerThread.setDaemon(true);
        consumerThread.start();

        return output;
    }

    private final BlockingQueue<Message> messages;

    private Output() {
        this.messages = new LinkedBlockingQueue<>();
    }

    public void emitCallResponse(Port.Command command, OtpErlangObject response) throws InterruptedException {
        this.emit(
                Erlang.tuple(
                        Erlang.atom("$kafka_consumer_response"),
                        Erlang.binary(command.ref()),
                        response
                )
        );
    }

    public void emit(OtpErlangObject message) throws InterruptedException {
        this.emit(message, false);
    }

    public void emit(OtpErlangObject message, boolean emitMetrics) throws InterruptedException {
        Long now = null;
        if (emitMetrics) {
            now = System.nanoTime();
        }

        this.messages.put(new Message(message, now));
    }

    @Override
    public void run() {
        // Writing to the file descriptor 4, which is allocated by Elixir for output
        try (var output = new DataOutputStream(new FileOutputStream("/dev/fd/4"))) {
            while (true) {
                var message = this.messages.take();

                // writing to the port is to some extent a blocking operation, so we measure it
                var sendingAt = System.nanoTime();
                this.notify(output, message.payload());
                var sentAt = System.nanoTime();

                if (message.startTime() != null) {
                    // send the measurement to the output
                    var duration = sentAt - message.startTime();
                    this.notify(
                            output,
                            Erlang.tuple(
                                    Erlang.atom("metrics"),
                                    Erlang.longValue(sentAt - sendingAt),
                                    Erlang.longValue(duration)
                            )
                    );
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void notify(DataOutputStream output, OtpErlangObject erlangTerm) throws IOException {
        byte[] payload = Erlang.encode(erlangTerm);

        // writing to the port

        // first four bytes are the length of the payload
        byte[] msgLength = java.nio.ByteBuffer.allocate(4).putInt(payload.length).array();
        output.write(msgLength);

        // then we write the actual payload
        output.write(payload);
    }

    private record Message(OtpErlangObject payload, Long startTime) {
    }
}
