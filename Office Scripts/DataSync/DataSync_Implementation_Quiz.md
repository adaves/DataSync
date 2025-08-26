# DataSync Implementation Quiz

## SQL Syntax and Access Database Handling

1. **Which of the following is a reserved word in MS Access that requires special handling in SQL queries?**
   - A) UserName
   - B) Count
   - C) ColumnData
   - D) RecordValue

2. **True or False: When using column aliases in MS Access SQL, it's always safe to enclose the alias in square brackets.**
   - A) True
   - B) False

3. **What SQL syntax should be used to properly reference a column named "ID" in a WHERE clause for MS Access?**
   - A) WHERE ID = 1
   - B) WHERE [ID] = 1
   - C) WHERE "ID" = 1
   - D) WHERE 'ID' = 1

4. **What is the correct way to specify a COUNT aggregate function with an alias in MS Access SQL?**
   - A) SELECT COUNT(*) as count FROM [table]
   - B) SELECT COUNT(*) as "count" FROM [table]
   - C) SELECT COUNT(*) as [count] FROM [table]
   - D) SELECT COUNT(*) as 'count' FROM [table]

5. **Which regular expression pattern would correctly identify column aliases in an SQL query?**
   - A) `r'\bCOLUMN\s+(\w+)\b'`
   - B) `r'\b(as|AS)\s+(\w+)\b'`
   - C) `r'SELECT\s+(\w+)'`
   - D) `r'\bFROM\s+(\w+)\b'`

## Transaction Management

6. **When implementing nested transactions, what should happen if an inner transaction fails?**
   - A) Only the inner transaction should be rolled back
   - B) The entire transaction chain should be rolled back
   - C) The outer transaction should continue regardless
   - D) A new transaction should be started automatically

7. **What does the error message "Already in rollback process, skipping recursive rollback" indicate?**
   - A) The transaction was successfully rolled back
   - B) An attempt to roll back a transaction that's already being rolled back
   - C) A deadlock has occurred in the database
   - D) The database connection is closed

8. **Which of the following is a best practice for transaction management in a database application?**
   - A) Always use auto-commit for all operations
   - B) Never use transactions for simple queries
   - C) Ensure proper nesting and coordination of transaction boundaries
   - D) Only use transactions for INSERT operations

9. **How should connection state be managed during a transaction error?**
   - A) The connection should always be closed immediately
   - B) The transaction should be rolled back, but the connection maintained
   - C) The connection should be reset but not closed
   - D) A new connection should be opened for each retry

## Error Handling

10. **What is the purpose of the `handle_error` decorator in the application?**
    - A) To log error messages
    - B) To retry failed operations
    - C) To properly manage transactions during errors
    - D) All of the above

11. **When a SQL syntax error occurs with code '42000', what is the most appropriate action?**
    - A) Immediately retry the query with the same syntax
    - B) Analyze and fix potential reserved word issues
    - C) Ignore the error and proceed with other operations
    - D) Permanently close the database connection

12. **True or False: It's sufficient to only handle ID as a reserved word in database queries.**
    - A) True
    - B) False

13. **What's the best approach to diagnose SQL syntax errors in MS Access?**
    - A) Try random syntax variations until one works
    - B) Log the exact query, analyze it for reserved words, and check syntax
    - C) Always use default SQL syntax
    - D) Avoid using column names that might be reserved words

## Implementation Concepts

14. **Why is it better to bracket all column identifiers in Access SQL rather than just reserved words?**
    - A) It improves query performance
    - B) It's required by the ODBC driver
    - C) It provides consistency and prevents errors if column names change
    - D) It reduces the query length

15. **Which design pattern is most appropriate for handling different SQL dialects like MS Access?**
    - A) Singleton
    - B) Factory
    - C) Strategy
    - D) Observer

16. **What is the benefit of centralizing SQL generation in a dedicated utility class?**
    - A) It reduces code duplication
    - B) It ensures consistent handling of SQL syntax across the application
    - C) It makes it easier to adapt to different database engines
    - D) All of the above

17. **When implementing batch operations against a database, what's an important consideration?**
    - A) Always process everything in a single transaction
    - B) Avoid using transactions entirely
    - C) Balance transaction size against the risk of losing work
    - D) Process one record at a time for maximum safety

18. **What approach should be used when a batch operation fails?**
    - A) Retry the exact same batch operation
    - B) Skip the failed records and continue
    - C) Implement smaller batches or individual processing with proper error recovery
    - D) Always fail the entire operation without any partial processing

19. **True or False: Using parameters in SQL queries is preferable to directly embedding values, especially in WHERE clauses.**
    - A) True
    - B) False

20. **Which component would be most appropriate for monitoring database operation performance?**
    - A) A simple log file
    - B) A metrics collection system with dashboards
    - C) Manual testing
    - D) Email alerts

## Answers

1. B
2. A
3. B
4. C
5. B
6. B
7. B
8. C
9. B
10. D
11. B
12. B
13. B
14. C
15. C
16. D
17. C
18. C
19. A
20. B 