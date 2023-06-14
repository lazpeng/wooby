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

- [ ] SELECT
    - [x] Basic column name, wildcard or expression
    - [x] Alias for each expression
    - [x] DISTINCT
        - [ ] DISTINCT ON() / DISTINCT()
    - [x] Single value subselect as expression
    - [ ] Expandable type system
    - [ ] More basic types
- [ ] Functions
    - [x] Basic functions
    - [x] Aggregate functions
    - [ ] "generic" functions (currently they all accept a certain type as parameter and return another. Need to overload for each possible combination)
    - [ ] (somewhat) complete set of standard functions (string and list manipulation, type casting, etc)
- [ ] FROM clause
    - [x] FROM table name
    - [ ] FROM sub select
- [ ] WITH clause and temp tables
    - [ ] (unlikely) recursive CTE
- [x] WHERE clause
- [x] GROUP BY expression
- [x] ORDER BY list of columns
    - [ ] ORDER BY list of expressions
- [ ] JOIN
    - [ ] LEFT/RIGHT JOIN
    - [ ] INNER/OUTER JOIN
    - [ ] Maybe CROSS and SELF JOIN?
- [ ] HAVING clause
- [ ] (big one) Data persistence and querying of data from disk
- [ ] System.Data interfaces implementation

And certainly more things I don't remember at the moment, or haven't thought of yet.
