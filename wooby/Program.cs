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

        static void PrintCommandOutput(ExecutionContext result)
        {
            var columns = new QueryOutputColumn[result.QueryOutput.Definition.Count];
            for (int i = 0; i < columns.Length; ++i)
            {
                var col = new QueryOutputColumn();
                var definition = result.QueryOutput.Definition[i];

                col.Title = definition.OutputName;
                col.Rows = result.QueryOutput.Rows.Select(r => r[i].PrettyPrint()).ToList();

                col.Length = Math.Max(col.Title.Length, col.Rows.Max(s => s.Length));
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
                            content = $"{i}".PadRight(maxLen);
                        } else content = "".PadRight(maxLen);
                    }
                    else if (i < 0)
                    {
                        var len = columns[j].Title.Length;
                        var rem = columns[j].Length - len;
                        var left = (int) Math.Floor(rem / 2.0) - 1;
                        content = columns[j].Title.PadLeft(len + left).PadRight(columns[j].Length);
                    } else
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
