using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using wooby;
using wooby.Database;
using wooby.Database.Persistence;
using wooby.Parsing;

namespace TerminalManager
{
    class Program
    {
        struct QueryOutputColumn
        {
            public int Length;
            public string Title;
            public List<string> Rows;
        }

        static void PrintQueryOutput(ExecutionContext result)
        {
            var columns = new QueryOutputColumn[result.QueryOutput.Definition.Count];
            for (int i = 0; i < columns.Length; ++i)
            {
                var col = new QueryOutputColumn();
                var definition = result.QueryOutput.Definition[i];

                col.Title = definition.OutputName;
                col.Rows = result.QueryOutput.Rows.Select(r => r[i].PrettyPrint()).ToList();

                col.Length = Math.Max(col.Title.Length, col.Rows.Count > 0 ? col.Rows.Max(s => s.Length) : 0);
                columns[i] = col;
            }

            var maxLen = columns[0].Rows.Count.ToString().Length;

            for (int i = -1; i < columns[0].Rows.Count; ++i)
            {
                for (int j = -1; j < columns.Length; ++j)
                {
                    string content;
                    if (j < 0)
                    {
                        if (i >= 0)
                        {
                            content = $"{i + 1}".PadRight(maxLen);
                        }
                        else content = "".PadRight(maxLen);
                    }
                    else if (i < 0)
                    {
                        var len = columns[j].Title.Length;
                        var rem = columns[j].Length - len;
                        var left = (int)Math.Floor(rem / 2.0) - 1;
                        content = columns[j].Title.PadLeft(len + left).PadRight(columns[j].Length);
                    }
                    else
                    {
                        content = columns[j].Rows[i].PadRight(columns[j].Length);
                    }

                    if (j < 0)
                    {
                        Console.Write("|");
                    }

                    Console.Write($" {content} |");
                }

                Console.WriteLine();
            }

            Console.WriteLine($"\nQuery returned {columns[0].Rows.Count} rows");
        }

        private static void PrintStatementOutput(Statement statement, ExecutionContext context)
        {
            if (statement.Kind == StatementKind.Query)
            {
                PrintQueryOutput(context);
            }
            else if (statement.Kind == StatementKind.Definition)
            {
                Console.WriteLine("Created.");
            }
            else if (statement.Kind == StatementKind.Manipulation)
            {
                Console.WriteLine($"{context.RowsAffected} rows affected.");
            }
        }

        private static void OpenDatabase(string line, ref Context context, ref Machine machine, ref IContextProvider contextProvider)
        {
            var filename = line[(line.IndexOf(' ') + 1)..];

            contextProvider = PersistenceBackendHelper.GetContextProvider(filename);
            if (contextProvider == null)
            {
                throw new Exception("Unrecognized database file. Make sure to use the right file extension");
            }

            context = contextProvider.LoadContext(filename);
            if (context == null)
            {
                throw new Exception("Failed to load database");
            }

            machine = new Machine();
            machine.Initialize(context);
        }

        private static void CreateDatabase(string line, ref Context context, ref Machine machine, ref IContextProvider contextProvider)
        {
            var firstSpaceIndex = line.IndexOf(' ') + 1;
            var type = line[firstSpaceIndex..line.IndexOf(' ', firstSpaceIndex)];
            var filename = line[(line.IndexOf(' ', firstSpaceIndex) + 1)..];

            contextProvider = PersistenceBackendHelper.GetContextProviderForType(type);
            if (contextProvider == null)
            {
                throw new Exception("Unrecognized database type");
            }

            var info = new FileInfo(filename);
            context = contextProvider.NewContext(info.Name, info.DirectoryName ?? "");
            if (context == null)
            {
                throw new Exception("Failed to load database");
            }

            machine = new Machine();
            machine.Initialize(context);
        }

        private static void PrintHelp()
        {
            Console.WriteLine("Commands:");
            Console.WriteLine("\t\\q or \\quit: Quit the application");
            Console.WriteLine("\t\\o or \\open [file]: Opens the given file as a database");
            Console.WriteLine("\t\\c or \\create [type] [name]: Creates a new database of the given type and name. Available types are:");
            Console.WriteLine("\t\tJSON: Saves the database as a json file");
            Console.WriteLine("\t\\close: Closes the current open database");
        }

        static void Main(string[] _)
        {
            Machine machine = null;
            Context context = null;
            IContextProvider contextProvider = null;
            var parser = new Parser();

            string input;
            bool quit = false;

            Console.WriteLine("Enter \\q to exit, \\h for help");

            while (!quit)
            {
                try
                {
                    Console.Write(">");
                    input = Console.ReadLine();

                    if (input.Length > 0 && input[0] == '\\')
                    {
                        var space = input.IndexOf(' ');
                        if (space <= 0)
                        {
                            space = input.Length;
                        }
                        var option = input.Substring(0, space);
                        switch (option)
                        {
                            case "\\q":
                            case "\\quit":
                                quit = true;
                                break;
                            case "\\h":
                            case "\\help":
                                PrintHelp();
                                break;
                            case "\\close":
                                if (machine == null || context == null || contextProvider == null)
                                {
                                    throw new Exception("No open database");
                                }

                                contextProvider.CommitChanges(context);

                                machine = null;
                                context = null;
                                contextProvider = null;
                                break;
                            case "\\o":
                            case "\\open":
                                OpenDatabase(input, ref context, ref machine, ref contextProvider);
                                break;
                            case "\\c":
                            case "\\create":
                                CreateDatabase(input, ref context, ref machine, ref contextProvider);
                                break;
                            default:
                                Console.WriteLine("Unrecognized option.");
                                break;
                        }
                    }
                    else
                    {
                        if (context == null || machine == null)
                        {
                            throw new Exception("No database selected. Create or open a database (\\help for more options)");
                        }
                        var statement = parser.ParseStatement(input, context);
                        var result = machine.Execute(statement);
                        PrintStatementOutput(statement, result);
                    }
                }
                catch (Exception e)
                {
                    var prev = Console.ForegroundColor;
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.Write("Error: ");
                    Console.ForegroundColor = prev;
                    Console.WriteLine($"{e.Message}\n{e.StackTrace}");
                }
            }
        }
    }
}
