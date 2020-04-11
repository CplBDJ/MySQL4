# MySQL4
Simple to use pymysql wrappers. An understanding of SQL is not required but makes use easier.

## Compatability
I tested this in 3.8.1. It uses f-strings so it should be compatible with 3.6+. Also uses __enter__ and __exit__ which I believe was first used in 3.5.

## Useability
Query classes
- Can be used with the python `with` statement.
- Can be used like a normal class.

## Examples
Find examples in the MySQL4.py file.

## Installation
Currently installing MySQL4 via pip is not supported.

- Install the pymysql module. `pip install pymysql`
- Place the *MySQL4.py* file in the same directory as the script.

## Why MySQL4?
- I had written another wrapper which had got to version 3.
- It was horribly bloated and wasn't well written.
- It supported python2, which is at it's end of life and f-strings make formatting text easy.
