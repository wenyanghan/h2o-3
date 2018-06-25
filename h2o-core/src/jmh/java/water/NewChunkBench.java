package water;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import water.fvec.Chunk;
import water.fvec.Vec;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

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
  private int rowInterval=1000;
  private double tolerance=1e-10;

  @Benchmark
  public double rawArrayRead() {
    double sum = 0;
    for (int col = 0; col < cols; ++col) {
      for (int row = 0; row < rows; ++row) {
        sum += raw[col][row];
      }
    }
    return sum;
  }

  @Benchmark
  public double rowsColsRead() {
    double sum = 0;
    for (int row = 0; row < rows; ++row) {
      for (int col = 0; col < cols; ++col) {
        sum += chunks[col].atd(row);
      }
    }
    return sum;
  }

  @Benchmark
  public double colsRowsRead() {
    double sum = 0;
    for (int col = 0; col < cols; ++col) {
      for (int row = 0; row < rows; ++row) {
        sum += chunks[col].atd(row);
      }
    }
    return sum;
  }

  @Benchmark
  public double colsRowsReadWithTypeDispatch() {
    double sum = 0;
    for (int col = 0; col < cols; ++col) {
      sum += walkChunk(rows, chunks[col]);
    }
    return sum;
  }

  @Benchmark
  public double colsRowsWithBulkRead() {
    double sum = 0;
    // Preallocate array for storing unpacked chunk data
    double [] vals = new double[chunks[0]._len];
    for (int col = 0; col < cols; ++col) {
      sum += walkChunkBulk(rows, chunks[col], vals);
    }
    return sum;
  }

  @Benchmark
  public double colsRowsReadWithFinalChunk() {
    double sum = 0;
    for (int col = 0; col < cols; ++col) {
      final Chunk c = chunks[col];
      for (int row = 0; row < rows; ++row) {
        sum += c.atd(row);
      }
    }
    return sum;
  }


  public void testsForLongs(boolean forConstants) {
    final long baseD = Long.MAX_VALUE-10*(long) rows;

      Vec tVec = Vec.makeZero(rows);
      Vec v;
      if (forConstants)
        v = new MRTask() {
          @Override public void map(Chunk cs) {
            for (int r=0; r<cs._len; r++){
              cs.set(r, baseD);
            }
          }
        }.doAll(tVec)._fr.vecs()[0];
      else
        v = new MRTask() {
          @Override public void map(Chunk cs) {
            long rowStart = cs.start();
            for (int r=0; r<cs._len; r++){
              cs.set(r, r+baseD+rowStart);
            }
          }
        }.doAll(tVec)._fr.vecs()[0];

      for (int rowInd=0; rowInd<rows; rowInd=rowInd+rowInterval) {
        if (forConstants)
          assertTrue("rowIndex: " + rowInd + " rowInd+baseD: " + (rowInd + baseD) + " v.at(rowIndex): "
                          + v.at8(rowInd) + " chk= " + v.elem2ChunkIdx(rowInd),
                  v.at8(rowInd) == baseD);
        else
          assertTrue("rowIndex: " + rowInd + " rowInd+baseD: " + (rowInd + baseD) + " v.at(rowIndex): "
                          + v.at8(rowInd) + " chk= " + v.elem2ChunkIdx(rowInd),
                  v.at8(rowInd) == (rowInd + baseD));
      }
    if (tVec != null)
      tVec.remove();
      if (v!=null)
        v.remove();
  }

  public void testsForIntegers(boolean forConstants){
    final int baseD = Integer.MAX_VALUE-2*rows;

      Vec tVec = Vec.makeZero(rows);
      Vec v;
      if (forConstants)
        v = new MRTask() {
          @Override
          public void map(Chunk cs) {
            for (int r = 0; r < cs._len; r++) {
              cs.set(r, baseD);
            }
          }
        }.doAll(tVec)._fr.vecs()[0];
      else
        v = new MRTask() {
          @Override
          public void map(Chunk cs) {
            long rowStart = cs.start();
            for (int r = 0; r < cs._len; r++) {
              cs.set(r, r + baseD + rowStart);
            }
          }
        }.doAll(tVec)._fr.vecs()[0];

      for (int rowInd=0; rowInd<rows; rowInd=rowInd+rowInterval) {
        if (forConstants)
          assertTrue("rowIndex: " + rowInd + " rowInd+baseD: " + (rowInd + baseD) + " v.at(rowIndex): "
                          + v.at8(rowInd) + " chk= " + v.elem2ChunkIdx(rowInd),
                  v.at8(rowInd) == baseD);
        else
          assertTrue("rowIndex: " + rowInd + " rowInd+baseD: " + (rowInd + baseD) + " v.at8(rowIndex): "
                          + v.at8(rowInd) + " chk= " + v.elem2ChunkIdx(rowInd),
                  v.at8(rowInd) == (rowInd + baseD));
      }

      if (tVec != null)
        tVec.remove();
      if (v != null)
        v.remove();
  }

  public void testsForDoubles(boolean forConstants, boolean bigDouble, boolean forFloat){

    final double baseD = bigDouble?(double) Long.MAX_VALUE+1:(forFloat?1.1:Math.PI);


      Vec tVec = Vec.makeZero(rows);
      Vec v;
      if (forConstants)
        v = new MRTask() {
          @Override
          public void map(Chunk cs) {
            for (int r = 0; r < cs._len; r++) {
              cs.set(r, baseD);
            }
          }
        }.doAll(tVec)._fr.vecs()[0];
      else
        v = new MRTask() {
          @Override
          public void map(Chunk cs) {
            long rowStart = cs.start();
            for (int r = 0; r < cs._len; r++) {
              cs.set(r, baseD + rowStart + r);
            }
          }
        }.doAll(tVec)._fr.vecs()[0];

      for (int rowInd = 0; rowInd < rows; rowInd = rowInd + rowInterval) {
        if (forConstants)
          assertTrue("rowIndex: " + rowInd + " rowInd+baseD: " + (baseD) + " v.at(rowIndex): "
                          + v.at(rowInd) + " chk= " + v.elem2ChunkIdx(rowInd),
                  Math.abs(v.at(rowInd) - baseD)/Math.max(v.at(rowInd), baseD) < tolerance);
        else
          assertTrue("rowIndex: " + rowInd + " rowInd+baseD: " + (baseD) + " v.at(rowIndex): "
                          + v.at(rowInd) + " chk= " + v.elem2ChunkIdx(rowInd),
                  (Math.abs(v.at(rowInd) - (baseD + rowInd)))/Math.max(v.at(rowInd), (baseD+rowInd)) < tolerance);
      }
      if (tVec != null)
        tVec.remove();
      if (v != null)
        v.remove();

  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(NewChunkBench.class.getSimpleName())
        .addProfiler(StackProfiler.class)
        .build();

    new Runner(opt).run();
  }
}
