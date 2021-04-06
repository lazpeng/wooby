# Wooby

My take on a database, without prior knowledge or guidance on the subject.
I'll keep this very short so basically I'm doing it in C# .NET for practicing my skills
with the ecosystem, and this is *not* meant to be taken seriously, to be performing,
feature complete or even accurate to the standards at the moment, just a funny side
project to see how hard it is to get to a somewhat barebones database.

My "roadmap" is as follows:

## v0.1
* Be able to run a simple select statement and get a simple result back, like CURRENT_DATE or selecting from a hard coded in-memory table for testing purposes. This means there's a little bit of everything working already, from lexer to the actual "virtual machine" executing the statement and I can start adding more features

## v0.2
* Again on SELECTs but more feature complete this time, with support for filtering and ordering the results. Also some virtual tables containing metadata information, not hardcoded but with actual data from the running schema

## v0.3
* Support for creating and dropping tables, inserting, updating and deleting rows from it. At this stage everything is still in-memory and no database data is written to the disk or persisted in any way

### Current stage: v0.1

I'll add more stuff as the project advances