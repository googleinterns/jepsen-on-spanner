package com.google.jepsenonspanner.operation;

import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;

public class TransactionalOperation extends Operation {
  
  // true means read, false means write
  public enum Type {
    READ,
    WRITE
  }

  private Type opType;

  // the operation that depends on this instance
  private TransactionalOperation dependent;

  // not null if this is a dependent operation; a function that returns the value that depends on
  // a previous operation (usually a read)
  private BinaryOperator<Long> findDependValFunc;

  // not null if this is a dependent operation; a function that decides whether current operation
  // should proceed, depending on the return value of the previous operation (usually a read)
  private BiPredicate<Long, Long> decideProceedFunc;

  /**
   * Constructor for a dependent transactional operation 
   * @param key 
   * @param value
   * @param opType
   * @param findDependValFunc
   * @param decideProceedFunc   
   */
  public TransactionalOperation(String key, int value, Type opType,
                                BinaryOperator<Long> findDependValFunc,
                                BiPredicate<Long, Long> decideProceedFunc) {
    this(key, value, opType, null, findDependValFunc, decideProceedFunc);
  }

  /**
   * Constructor for a non-dependent transactional operation
   * @param key
   * @param value
   * @param opType
   */
  public TransactionalOperation(String key, int value, Type opType) {
    this(key, value, opType, null, null, null);
  }

  /**
   * Base constructor
   * @param key 
   * @param value
   * @param opType
   * @param dependent
   * @param findDependValFunc
   * @param decideProceedFunc
   */
  public TransactionalOperation(String key, int value, Type opType,
                                TransactionalOperation dependent,
                                BinaryOperator<Long> findDependValFunc,
                                BiPredicate<Long, Long> decideProceedFunc) {
    super(key, value);
    this.opType = opType;
    this.dependent = dependent;
    this.findDependValFunc = findDependValFunc;
    this.decideProceedFunc = decideProceedFunc;
  }
  
  public static TransactionalOperation createTransactionalRead(String key) {
    return new TransactionalOperation(key, 0, Type.READ, null, null, null);
  }

  public static TransactionalOperation createTransactionalWrite(String key, int value) {
    return new TransactionalOperation(key, value, Type.WRITE, null, null, null);
  }

  public static TransactionalOperation createDependentTransactionalWrite(String key, int value,
                                                                         BinaryOperator<Long> findDependValFunc,
                                                                         BiPredicate<Long, Long> decideProceedFunc) {
    return new TransactionalOperation(key, value, Type.WRITE, null, findDependValFunc, decideProceedFunc);
  }
  
  /**
   * Decides if current operation should be executed
   *
   * @param dependOn return value of the operation this depends on
   */
  public boolean decideProceed(long dependOn) {
    if (decideProceedFunc == null) return true;
    return decideProceedFunc.test(dependOn, getValue());
  }

  /**
   * Fills in the value depending on return value of the previous operation
   *
   * @param dependOn return value of the operation this depends on
   */
  public void findDependentValue(long dependOn) {
    if (findDependValFunc == null) return;
    setValue(findDependValFunc.apply(dependOn, getValue()));
  }

  /**
   * Sets the dependent that relies on this operation
   *
   * @param dependent the dependent operation
   */
  public void setDependentOp(TransactionalOperation dependent) {
    this.dependent = dependent;
  }

  /**
   * Returns the dependent operation that relies on this operation
   */
  public TransactionalOperation getDependentOp() {
    return dependent;
  }

  public boolean isRead() {
    return opType == Type.READ;
  }

  @Override
  public String toString() {
    return String.format("%s %s, dependent = [ %s ]", isRead() ? "Strong Read" : "Write",
            super.toString(), dependent.toString());
  }
}
