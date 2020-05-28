package com.google.jepsenonspanner.operation;

import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;

public class TransactionalOperation extends Operation {
  
  // true means read, false means write
  private boolean isRead;
  public static final boolean READ = true;
  public static final boolean WRITE = false;

  // the operation that depends on this instance
  private TransactionalOperation dependent;

  // not null if this is a dependent operation; a function that returns the value that depends on
  // a previous operation (usually a read)
  private BinaryOperator<Integer> findDependValFunc;

  // not null if this is a dependent operation; a function that decides whether current operation
  // should proceed, depending on the return value of the previous operation (usually a read)
  private BiPredicate<Integer, Integer> decideProceedFunc;

  /**
   * Constructor for a dependent transactional operation 
   * @param key 
   * @param value
   * @param isRead
   * @param findDependValFunc
   * @param decideProceedFunc   
   */
  public TransactionalOperation(String key, int value, boolean isRead,
                                BinaryOperator<Integer> findDependValFunc,
                                BiPredicate<Integer, Integer> decideProceedFunc) {
    this(key, value, isRead, null, findDependValFunc, decideProceedFunc);
  }

  /**
   * Constructor for a non-dependent transactional operation
   * @param key
   * @param value
   * @param isRead
   */
  public TransactionalOperation(String key, int value, boolean isRead) {
    this(key, value, isRead, null, null, null);
  }

  /**
   * Base constructor
   * @param key 
   * @param value
   * @param isRead
   * @param dependent
   * @param findDependValFunc
   * @param decideProceedFunc
   */
  public TransactionalOperation(String key, int value, boolean isRead,
                                TransactionalOperation dependent,
                                BinaryOperator<Integer> findDependValFunc,
                                BiPredicate<Integer, Integer> decideProceedFunc) {
    super(key, value);
    this.isRead = isRead;
    this.dependent = dependent;
    this.findDependValFunc = findDependValFunc;
    this.decideProceedFunc = decideProceedFunc;
  }
  
  
  
  /**
   * Decides if current operation should be executed
   *
   * @param dependOn return value of the operation this depends on
   */
  public boolean decideProceed(int dependOn) {
    return decideProceedFunc.test(dependOn, getValue());
  }

  /**
   * Fills in the value depending on return value of the previous operation
   *
   * @param dependOn return value of the operation this depends on
   */
  public void findDependentValue(int dependOn) {
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
   * Return the dependent operation that relies on this operation
   */
  public TransactionalOperation getDependentOp() {
    return dependent;
  }

  public boolean isRead() {
    return isRead;
  }

  @Override
  public String toString() {
    return String.format("%s %s, dependent = [ %s ]", isRead ? "Strong Read" : "Write",
            super.toString(), dependent.toString());
  }
}
