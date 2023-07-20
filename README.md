# Wooby

My take on a database, without prior knowledge or guidance on the subject.
I'll keep this very short so basically I'm doing it in C# .NET for practicing my skills
with the ecosystem, and this is *not* meant to be taken seriously, to be performing,
feature complete or even accurate to any standards at the moment, just a funny side
project to see how hard it is to get to a somewhat barebones database.

I won't try to mimic an existing RDBMS, nor implement everything different from everyone else. But it probably leans towards Oracle the most, followed by SQLite, which are the ones I have the most experience with.

It's currently more of a SQL interpreter than a database, though I intend to focus more on the storage and querying performance side of things as soon as I have implemented enough SQL features to test those with.

Expanding a bit on the first paragraph, I also intent to test the feasibility or even possibility of implementing a few things that cross or have crossed my mind these past years. Moments I thought (specially during a time I heavily dealt with and maintained a somewhat large codebase of Oracle PL/SQL) "what if this could be done entirely in the database instead?". Probably the opposite of what most people, and myself in other occasions (when not entirely out of amusement for the idea), think.

_Removed the roadmap. Instead I'll list below what's already "working" and what I intend to implement in the future_

_unlikely = low priority, maybe never_

- [ ] SELECT
    - [x] Basic column name, wildcard or expression
    - [x] Alias for each expression
    - [x] DISTINCT
    - [ ] COUNT
        - [x] Basic COUNT support, without any arguments, counts all rows in the resulting query
        - [ ] COUNT(expr) Number of rows where _expr_ is not NULL (can be achieved with WHERE _expr_ IS NOT NULL)
        - [ ] COUNT(DISTINCT expr) same as above but count only distinct values of _expr_ (can be achieved via existing group by clause)
        - [ ] COUNT(*) syntactic sugar, does the same as _Basic COUNT_ above
        - [ ] (unlikely) OVER () / PARTITION
    - [ ] UNION clause
        - [ ] UNION ALL
        - [ ] UNION (filtering so that union'ed rows are not duplicate in the output)
    - [ ] Better semantic analysis for Boolean expressions (ensure expression is valid)
    - [x] Single value subselect as expression
    - [x] Expandable type system
        - [ ] Custom conversion between types
    - [ ] More basic types
    - [ ] Type promotion/casting (e.g. CURRENT_DATE() - 30)
- [ ] INSERT
    - [x] Basic INSERT support with columns and values list
    - [ ] Batch insert (VALUES (...), (...), ...)
    - [ ] INSERT from select (INSERT INTO x SELECT ... FROM y)
- [ ] UPDATE
    - [x] Basic update support with SET list of columns
- [ ] DELETE
    - [x] Basic DELETE support
- [ ] Functions
    - [x] Basic functions
    - [x] Aggregate functions
    - [ ] "generic" functions (currently they all accept a certain type as parameter and return another. Need to overload for each possible combination)
    - [ ] (somewhat) complete set of standard functions (string and list manipulation, type casting, etc)
- [x] FROM clause
    - [x] FROM table name
    - [x] FROM sub select
- [ ] WITH clause and temp tables
    - [ ] (unlikely) recursive CTE
- [x] WHERE clause
    - [ ] IS / IS NOT for NULL comparison (currently works with = and !=/<>)
- [x] GROUP BY expression
- [x] ORDER BY expression
- [ ] JOIN
    - [x] LEFT JOIN
    - [x] INNER JOIN
    - [ ] RIGHT JOIN
    - [ ] OUTER JOIN
    - [ ] (unlikely) CROSS and SELF JOIN
- [ ] HAVING clause
- [ ] Meta tables with info about the schema and database
- [ ] Explain execution plan
- [ ] Import CSV files into tables from the command line
- [ ] Command line that doesn't suck
- [ ] (big one) Data persistence and querying of data from disk
- [ ] System.Data interfaces implementation
- [ ] GUI query runner and schema browser
- [x] Unhelpful and vague error messages

And certainly more things I don't remember at the moment, or haven't thought of yet.
