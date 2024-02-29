# Example:  SingleStore Collocated Service Using Binary Protocol

## Summary

**Attention**: The code in this repository is intended for experimental use only and is not fully tested, documented, or supported by SingleStore. Visit the [SingleStore Forums](https://www.singlestore.com/forum/) to ask questions about this repository.

This is a simple Python program that illustrates a SingleStore collocated
process using the binary protocol (via Unix sockets and shared memory) and the
efficient ROWDAT_1 format.

It simply returns each row it receives with the prefix `HELLO `.

## Usage

To run this example, you'll need to do the following:
1. Copy this program to each node of your database cluster.
1. Run this program on each node of your database cluster using `python3 xfunc.py`.

Once the example is running, you can create the external function in SQL like this:
```sql
CREATE EXTERNAL FUNCTION xfunc(a text) RETURNS TEXT AS COLLOCATED SERVICE '/tmp/xfunc_pipe' FORMAT ROWDAT_1;
```

Finally, you can run the following SQL commands to test it:
```sql
CREATE DATABASE test;

CREATE TABLE foobar(a TEXT);

INSERT INTO foobar(a) VALUES ('one'), ('two'), ('three'), ('four'), ('five'), ('six'), ('seven'), ('eight'), ('nine'), ('ten');

CREATE EXTERNAL FUNCTION xfunc(a text) RETURNS TEXT AS COLLOCATED SERVICE '/tmp/xfunc_pipe' FORMAT ROWDAT_1;

SELECT xfunc(a) FROM foobar;
```

## Product Documentation:
[External Functions](https://docs.singlestore.com/cloud/reference/sql-reference/procedural-sql-reference/create-or-replace-external-function/)
[Collocated Service](https://docs.singlestore.com/cloud/reference/sql-reference/procedural-sql-reference/create-or-replace-external-function/?_gl=1*ln41ot*_up*MQ..*_ga*MTQ4ODY3MTk2Mi4xNjk4MjQzNDc5*_ga_V9YBY81TXW*MTY5ODI0MzQ3OS4xLjAuMTY5ODI0MzQ3OS4wLjAuMA..#rowdat-1)
[ROWDAT_1 Format](https://docs.singlestore.com/cloud/reference/sql-reference/procedural-sql-reference/create-or-replace-external-function/?_gl=1*ln41ot*_up*MQ..*_ga*MTQ4ODY3MTk2Mi4xNjk4MjQzNDc5*_ga_V9YBY81TXW*MTY5ODI0MzQ3OS4xLjAuMTY5ODI0MzQ3OS4wLjAuMA..#using-collocated-service)

