package water;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static water.TestUtil.parse_test_file;

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
  private int rowInterval = 1000;
  private double tolerance = 1e-10;
  private String[] filenames = new String[]{"smalldata/jira/floatVals.csv", "smalldata/jira/integerVals.csv",
          "smalldata/jira/longVals.csv", "smalldata/jira/doubleVals.csv", "smalldata/jira/bigDoubleVals.csv"};
  private int fileRows = 100000;

  @Benchmark
  public void testParseFloats() {
    testsForDoubles(false, false, true);
    testsForDoubles(true, false, true);
    testParseFromFiles(0);
  }

  @Benchmark
  public void testParseDoubles() {
    testsForDoubles(false, false, false);
    testsForDoubles(true, false, false);  // constants
    testParseFromFiles(3);
  }

  @Benchmark
  public void testParseBigDoubles() {
    testsForDoubles(false, true, false);
    testsForDoubles(true, true, false);
    testParseFromFiles(4);
  }

  @Benchmark
  public void testParseInteger() {
    testsForIntegers(false);
    testsForIntegers(true);
    testParseFromFiles(1);
  }

  // todo: enable benchmark test after Spencer PR is in.
 // @Benchmark
  public void testParseLong() {
    testsForLongs(false);
    testsForLongs(true);
    testParseFromFiles(2);
  }


  public void testParseFromFiles(int index) {
    Frame f = parse_test_file(filenames[index]);  // parse from file
    assertTrue(f.numRows() == fileRows);
    if (f != null)
      f.delete();
  }

  public void testsForLongs(boolean forConstants) {
    final long baseD = Long.MAX_VALUE - 10 * (long) rows;

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

    for (int rowInd = 0; rowInd < rows; rowInd = rowInd + rowInterval) {
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
    if (v != null)
      v.remove();
  }

  public void testsForIntegers(boolean forConstants) {
    final int baseD = Integer.MAX_VALUE - 2 * rows;

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

    for (int rowInd = 0; rowInd < rows; rowInd = rowInd + rowInterval) {
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

  public void testsForDoubles(boolean forConstants, boolean bigDouble, boolean forFloat) {

    final double baseD = bigDouble ? (double) Long.MAX_VALUE + 1 : (forFloat ? 1.1 : Math.PI);


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
                Math.abs(v.at(rowInd) - baseD) / Math.max(v.at(rowInd), baseD) < tolerance);
      else
        assertTrue("rowIndex: " + rowInd + " rowInd+baseD: " + (baseD) + " v.at(rowIndex): "
                        + v.at(rowInd) + " chk= " + v.elem2ChunkIdx(rowInd),
                (Math.abs(v.at(rowInd) - (baseD + rowInd))) / Math.max(v.at(rowInd), (baseD + rowInd)) < tolerance);
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
