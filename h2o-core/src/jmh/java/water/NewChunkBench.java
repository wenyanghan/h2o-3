package water;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import water.fvec.Chunk;
import water.fvec.NewChunk;

import java.util.concurrent.TimeUnit;

/**
 * Chunk access patterns benchmark
 */
@State(Scope.Thread)
//@Fork(value = 1, jvmArgsAppend = "-XX:+PrintCompilation")
@Fork(value = 1, jvmArgsAppend = "-Xmx12g")
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)

public class NewChunkBench {

  @Param({"10000", "1000000"})
  private int rows;
  private double[][] raw;
  private int cols = 7;
  private static double baseD = Math.PI;
  private static double baseF = 1.1;

  @Benchmark
  public void writeIntegers() {
    Chunk[] chunks = new Chunk[2];
    for (int col = 0; col < 2; ++col) {
      chunks[col] = new NewChunk(raw[col]).compress();
    }
  }

  @Benchmark
  public void writeFloats() {
    Chunk chunks = new NewChunk(raw[4]).compress();
  }

  @Benchmark
  public void writeDoubles() {
    Chunk chunks = new NewChunk(raw[5]).compress();
  }

  @Benchmark
  public void writeBigLong() {
    Chunk chunks = new NewChunk(raw[6]).compress();

  }

  @Benchmark
  public void writeLongs() {
    Chunk chunks = new NewChunk(raw[2]).compress();
  }

  @Benchmark
  public void writeSparse() {
    Chunk chunks = new NewChunk(raw[3]).compress();
  }


  @Setup
  public void setup() {
    raw = new double[cols][rows]; // generate data in double array
    for (int col = 0; col < cols; ++col) {
      for (int row = 0; row < rows; ++row) {
        raw[col][row] = get(col, row);
      }
    }
  }

  private static double get(int j, int i) {
    switch (j % 4) { // do 4 chunk types
      case 0:
        return i % 200; //C1NChunk - 1 byte integer
      case 1:
        return i % 500; //C2Chunk - 2 byte integer
      case 2:
        return  i+Integer.MAX_VALUE;  // long
      case 3:
        return i == 17 ? 1 : 0; //CX0Chunk - sparse
      case 4:
        return baseF+i;  // float point
      case 5:
        return baseD+i; // double
      case 6: // integer exceeding long
        return Long.MAX_VALUE+i;
      default:
        throw H2O.unimpl();
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(ChunkBench.class.getSimpleName())
            .addProfiler(StackProfiler.class)
            .build();

    new Runner(opt).run();
  }
}