package DSPPCode.flink.digital_conversion.question;

import org.apache.flink.api.common.functions.Partitioner;


public abstract class DigitalPartitioner<T> implements Partitioner<T> {

  /**
   * Computes the partition for the given key.
   *
   * @param key           The key.
   * @param numPartitions The number of partitions to partition into.
   * @return The partition index.
   */
  @Override
  public abstract int partition(T key, int numPartitions);
}
