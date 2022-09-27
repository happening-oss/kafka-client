package com.superology.kafka;

import java.util.*;
import java.util.concurrent.*;

/*
 * The worker thread of a port program. See {@link PortDriver} for details.
 */
public class PortWorker implements Runnable {
  public static PortWorker start(Port port, PortOutput output, Object[] args) {
    var worker = new PortWorker(port, output, args);

    // Using a daemon thread to ensure program termination if the main thread stops.
    var consumerThread = new Thread(worker);
    consumerThread.setDaemon(true);
    consumerThread.start();

    return worker;
  }

  private BlockingQueue<Port.Command> commands = new LinkedBlockingQueue<>();
  private Port port;
  private PortOutput output;
  private Object[] args;

  private PortWorker(Port port, PortOutput output, Object[] args) {
    this.port = port;
    this.output = output;
    this.args = args;
  }

  @Override
  public void run() {
    try {
      var exitStatus = port.run(this, output, args);
      System.exit(exitStatus);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  public void sendCommand(Port.Command command) {
    commands.add(command);
  }

  public Collection<Port.Command> drainCommands() {
    var result = new ArrayList<Port.Command>();
    commands.drainTo(result);
    return result;
  }

  public Port.Command take() throws InterruptedException {
    return commands.take();
  }
}
