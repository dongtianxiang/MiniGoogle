package edu.upenn.cis.stormlite.infrastructure;

public interface Context {

  void write(String key, String value);
  
}
