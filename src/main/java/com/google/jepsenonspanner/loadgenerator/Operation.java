package com.google.jepsenonspanner.loadgenerator;

import java.util.Arrays;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;

/**
 * Operation encapsulates an operation produced by a generator that needs to be executed by the
 * client wrapper. It boils down to three types of basic interaction with the spanner instance:
 * strong read, stale read and write. All user-defined loads build on top of this class in the
 * form of a list of operations that will be performed in the same transaction, or a series of
 * stale reads.
 * <p></p>
 * An operation may have dependents whose value will be filled in by the return value of this
 * operation. The client should discover such dependents and call the two member functions to
 * fill in the value. In the context of this file, a dependent operation is one that depends on
 * the return value of a previous operation.
 */
public class Operation {

  /** Enum type to identify read or write */
  public enum OpType {
    READ,
    WRITE
  }

  private OpType op;
  private String key;
  private int value;
  private boolean bounded;

  // nonzero value means stale read
  private int millisecondsPast;

  // the operation that depends on this instance
  private Operation dependent;

  // not null if this is a dependent operation; a function that returns the value that depends on
  // a previous operation (usually a read)
  private BinaryOperator<Integer> findDependValFunc;

  // not null if this is a dependent operation; a function that decides whether current operation
  // should proceed, depending on the return value of the previous operation (usually a read)
  private BiPredicate<Integer, Integer> decideProceedFunc;

  /**
   * Base constructor
   *
   * @param op the operation type
   * @param key key to operate on
   * @param value value to execute (null for read)
   * @param millisecondPast how many milliseconds in the past to read
   * @param dependent operation that depends on this instance
   * @param findDependValFunc function that returns the value that depends on a previous operation
   * @param decideProceedFunc function that decides whether current operation should proceed
   */
  public Operation(OpType op, String key, int value, int millisecondPast, Operation dependent,
                   BinaryOperator<Integer> findDependValFunc,
                   BiPredicate<Integer, Integer> decideProceedFunc, boolean bounded) {
    this.op = op;
    this.key = key;
    this.value = value;
    this.millisecondsPast = millisecondPast;
    this.dependent = dependent;
    this.findDependValFunc = findDependValFunc;
    this.decideProceedFunc = decideProceedFunc;
    this.bounded = bounded;
  }

  /**
   * Constructor for a non-dependent stale read
   * 
   * @see Operation#Operation(OpType, String, int, int, Operation, BinaryOperator, BiPredicate, boolean)
   */
  public Operation(OpType op, String key, int value, int millisecondsPast, boolean bounded) {
    this(op, key, value, millisecondsPast, null, null, null, bounded);
  }

  /**
   * Constructor for a dependent transactional operation
   *
   * @see Operation#Operation(OpType, String, int, int, Operation, BinaryOperator, BiPredicate, boolean)
   */
  public Operation(OpType op, String key, int value,
                   BinaryOperator<Integer> findDependValFunc,
                   BiPredicate<Integer, Integer> decideProceedFunc) {
    this(op, key, value, 0, null, findDependValFunc, decideProceedFunc, false);
  }

  /**
   * Constructor for a non-dependent transactional operation
   *
   * @see Operation#Operation(OpType, String, int, int, Operation, BinaryOperator, BiPredicate, boolean)
   */
  public Operation(OpType op, String key, int value) {
    this(op, key, value, 0, null, null, null, false);
  }

  public OpType getOp() {
    return op;
  }

  public void setOp(OpType op) {
    this.op = op;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }

  public int getMillisecondsPast() {
    return millisecondsPast;
  }

  public void setMillisecondsPast(int millisecondsPast) {
    this.millisecondsPast = millisecondsPast;
  }

  /**
   * Decides if current operation should be executed
   *
   * @param dependOn return value of the operation this depends on
   */
  public boolean decideProceed(int dependOn) {
    return decideProceedFunc.test(dependOn, value);
  }

  /**
   * Fills in the value depending on return value of the previous operation
   *
   * @param dependOn return value of the operation this depends on
   */
  public void findDependentValue(int dependOn) {
    value = findDependValFunc.apply(dependOn, value);
  }

  /**
   * Sets the dependent that relies on this operation
   *
   * @param dependent the dependent operation
   */
  public void setDependentOp(Operation dependent) {
    this.dependent = dependent;
  }

  /**
   * Return the dependent operation that relies on this operation
   */
  public Operation getDependentOp() {
    return dependent;
  }

  /**
   * Return a string representation of this operation
   */
  @Override
  public String toString() {
    return String.join(" ", Arrays.asList(op.name(), key, Integer.toString(value),
            Integer.toString(millisecondsPast), "[", String.valueOf(dependent), "]"));
  }
}
