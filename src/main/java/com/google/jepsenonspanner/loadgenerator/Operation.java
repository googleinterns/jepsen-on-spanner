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
  private String value;

  // nonzero value means stale read
  private int millisecondsPast;

  // the operation that depends on this instance
  private Operation dependent;

  // not null if this is a dependent operation; a function that returns the value that depends on
  // a previous operation (usually a read)
  private BinaryOperator<String> findDependValFunc;

  // not null if this is a dependent operation; a function that decides whether current operation
  // should proceed, depending on the return value of the previous operation (usually a read)
  private BiPredicate<String, String> decideProceedFunc;

  /**
   * Base constructor
   *
   * @param op_ the operation type
   * @param key_ key to operate on
   * @param value_ value to execute (null for read)
   * @param millisecondPast_ how many milliseconds in the past to read
   * @param dependent_ operation that depends on this instance
   * @param findDependValFunc_ function that returns the value that depends on a previous operation
   * @param decideProceedFunc_ function that decides whether current operation should proceed
   */
  public Operation(OpType op_, String key_, String value_, int millisecondPast_,
                   Operation dependent_, BinaryOperator<String> findDependValFunc_,
                   BiPredicate<String, String> decideProceedFunc_) {
    op = op_;
    key = key_;
    value = value_;
    millisecondsPast = millisecondPast_;
    dependent = dependent_;
    findDependValFunc = findDependValFunc_;
    decideProceedFunc = decideProceedFunc_;
  }

  /**
   * Constructor for a non-dependent stale read
   * 
   * @see Operation#Operation(OpType, String, String, int, Operation, BinaryOperator, BiPredicate)
   */
  public Operation(OpType op_, String key_, String value_, int millisecondsPast_) {
    this(op_, key_, value_, millisecondsPast_, null, null, null);
  }

  /**
   * Constructor for a dependent transactional operation
   *
   * @see Operation#Operation(OpType, String, String, int, Operation, BinaryOperator, BiPredicate)
   */
  public Operation(OpType op_, String key_, String value_,
                   BinaryOperator<String> findDependValFunc_,
                   BiPredicate<String, String> decideProceedFunc_) {
    this(op_, key_, value_, 0, null, findDependValFunc_, decideProceedFunc_);
  }

  /**
   * Constructor for a non-dependent transactional operation
   *
   * @see Operation#Operation(OpType, String, String, int, Operation, BinaryOperator, BiPredicate)
   */
  public Operation(OpType op_, String key_, String value_) {
    this(op_, key_, value_, 0, null, null, null);
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

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
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
  public boolean decideProceed(String dependOn) {
    return decideProceedFunc.test(dependOn, value);
  }

  /**
   * Fill in the value depending on return value of the previous operation
   *
   * @param dependOn return value of the operation this depends on
   */
  public void findDependentValue(String dependOn) {
    value = findDependValFunc.apply(dependOn, value);
  }

  /**
   * Sets the dependent that relies on this operation
   *
   * @param dependent_ the dependent operation
   */
  public void setDependentOp(Operation dependent_) {
    dependent = dependent_;
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
    return String.join(" ", Arrays.asList(op.name(), key, value,
            Integer.toString(millisecondsPast), "[", String.valueOf(dependent), "]"));
  }
}
