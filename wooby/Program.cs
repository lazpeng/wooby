using System;
using System.Collections.Generic;
using System.Linq;

namespace wooby
{
    class Program
    {
        struct QueryOutputColumn
        {
            public int Length;
            public string Title;
            public List<string> Rows;
        }

        static string PrettyPrint(ColumnValue value)
        {
            return value.Kind switch
            {
                ValueKind.Null => "<NULL>",
                ValueKind.Number => $"{value.Number}",
                ValueKind.Text => value.Text,
                _ => ""
            };
        }

        static void PrintCommandOutput(ExecutionContext result)
        {
            var columns = new QueryOutputColumn[result.QueryOutput.Definition.Count];
            for (int i = 0; i < columns.Length; ++i)
            {
                var col = new QueryOutputColumn();
                var definition = result.QueryOutput.Definition[i];

                col.Title = definition.OutputName;
                col.Rows = result.QueryOutput.Rows.Select(r => PrettyPrint(r[i])).ToList();

                col.Length = Math.Max(col.Title.Length, col.Rows.Max(s => s.Length));
                columns[i] = col;
            }

            for (int i = -1; i < columns[0].Rows.Count; ++i)
            {
                for (int j = 0; j < columns.Length; ++j)
                {
                    string content;
                    if (i < 0)
                    {
                        content = columns[j].Title;
                    } else
                    {
                        content = columns[j].Rows[i];
                    }

                    if (j == 0)
                    {
                        Console.Write("|");
                    }

                    Console.Write($" {content.PadRight(columns[j].Length)} |");
                }

                Console.WriteLine();
            }

            Console.WriteLine($"\nQuery returned {columns[0].Rows.Count} rows");
        }

        static void Main(string[] args)
        {
            var machine = new Machine();
            var parser = new Parser();
            var context = machine.Initialize();

            string input;
            bool quit = false;

            Console.WriteLine("Enter \\q to exit");

            while (!quit)
            {
                Console.Write(">");
                input = Console.ReadLine();

                if (input.Trim() == "\\q")
                {
                    quit = true;
                } else
                {
                    var cmd = parser.ParseCommand(input, context);
                    var result = machine.Execute(Compiler.CompileCommand(cmd, context));
                    PrintCommandOutput(result);
                }
            }
        }
    }
}
