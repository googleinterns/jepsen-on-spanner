package com.google.jepsenonspanner.operation;

import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;

public class TransactionalAction {

  private String key;
  private long value;
  
  // true means read, false means write
  public enum Type {
    READ,
    WRITE
  }

  private Type actionType;

  // the operation that depends on this instance
  private TransactionalAction dependent;

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
   * @param actionType
   * @param findDependValFunc
   * @param decideProceedFunc   
   */
  public TransactionalAction(String key, int value, Type actionType,
                             BinaryOperator<Long> findDependValFunc,
                             BiPredicate<Long, Long> decideProceedFunc) {
    this(key, value, actionType, null, findDependValFunc, decideProceedFunc);
  }

  /**
   * Constructor for a non-dependent transactional operation
   * @param key
   * @param value
   * @param actionType
   */
  public TransactionalAction(String key, int value, Type actionType) {
    this(key, value, actionType, null, null, null);
  }

  /**
   * Base constructor
   * @param key 
   * @param value
   * @param actionType
   * @param dependent
   * @param findDependValFunc
   * @param decideProceedFunc
   */
  public TransactionalAction(String key, int value, Type actionType,
                             TransactionalAction dependent,
                             BinaryOperator<Long> findDependValFunc,
                             BiPredicate<Long, Long> decideProceedFunc) {
    this.key = key;
    this.value = value;
    this.actionType = actionType;
    this.dependent = dependent;
    this.findDependValFunc = findDependValFunc;
    this.decideProceedFunc = decideProceedFunc;
  }
  
  public static TransactionalAction createTransactionalRead(String key) {
    return new TransactionalAction(key, 0, Type.READ, null, null, null);
  }

  public static TransactionalAction createTransactionalWrite(String key, int value) {
    return new TransactionalAction(key, value, Type.WRITE, null, null, null);
  }

  public static TransactionalAction createDependentTransactionalWrite(String key, int value,
                                                                      BinaryOperator<Long> findDependValFunc,
                                                                      BiPredicate<Long, Long> decideProceedFunc) {
    return new TransactionalAction(key, value, Type.WRITE, null, findDependValFunc, decideProceedFunc);
  }
  
  /**
   * Decides if current operation should be executed
   *
   * @param dependOn return value of the operation this depends on
   */
  public boolean decideProceed(long dependOn) {
    if (decideProceedFunc == null) return true;
    return decideProceedFunc.test(dependOn, value);
  }

  /**
   * Fills in the value depending on return value of the previous operation
   *
   * @param dependOn return value of the operation this depends on
   */
  public void findDependentValue(long dependOn) {
    if (findDependValFunc == null) return;
    this.value = findDependValFunc.apply(dependOn, value);
  }

  /**
   * Sets the dependent that relies on this operation
   *
   * @param dependent the dependent operation
   */
  public void setDependentAction(TransactionalAction dependent) {
    this.dependent = dependent;
  }

  /**
   * Returns the dependent operation that relies on this operation
   */
  public TransactionalAction getDependentAction() {
    return dependent;
  }

  public boolean isRead() {
    return actionType == Type.READ;
  }

  public String getKey() {
    return key;
  }

  public long getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("%s %s %s, dependent = [ %s ]", isRead() ? "Strong Read" : "Write",
            key, value, String.valueOf(dependent));
  }
}
