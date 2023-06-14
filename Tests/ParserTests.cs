using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using wooby;
using wooby.Database;
using wooby.Parsing;

namespace Tests
{
    [TestClass]
    public class ParserTests
    {
        [TestMethod]
        public void TestOperatorParsing()
        {
            var input = "   +   ";

            var next = new Parser().NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.Operator);
            Assert.AreEqual(next.OperatorValue, Operator.Plus);
        }

        [TestMethod]
        public void TestTokenDetection()
        {
            var input = " 'testing' SELECT 2.45 symbol";

            var next = new Parser().NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.LiteralString);

            int offset = next.InputLength;

            next = new Parser().NextToken(input, offset);
            Assert.AreEqual(next.Kind, Parser.TokenKind.Keyword);
            offset += next.InputLength;

            next = new Parser().NextToken(input, offset);
            Assert.AreEqual(next.Kind, Parser.TokenKind.LiteralNumber);
            offset += next.InputLength;

            next = new Parser().NextToken(input, offset);
            Assert.AreEqual(next.Kind, Parser.TokenKind.Symbol);
        }

        [TestMethod]
        public void TestString()
        {
            var input = " 'test string' ";

            var next = new Parser().NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.LiteralString);
            Assert.AreEqual(next.StringValue, "test string");
        }

        [TestMethod]
        public void TestSymbol()
        {
            var input = " test another ";

            var next = new Parser().NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.Symbol);
            Assert.AreEqual(next.StringValue, "test");
        }

        [TestMethod]
        public void TestBasicColumnReference()
        {
            var ctx = new Machine().Initialize();
            var table = new TableMeta() { Name = "table" };
            ctx.AddColumn(new ColumnMeta() { Name = "a", Type = ColumnType.Number }, table);
            ctx.AddTable(table);

            var input = "table.a";

            var parser = new Parser();
            parser.AddSource(new ColumnReference() { Table = "table" });
            var reference = parser.ParseReference(input, 0, ctx, new Parser.ReferenceFlags() { ResolveReferences = true });
            var expected = new ColumnReference() { Column = "a", Table = "table", InputLength = input.Length };

            Assert.AreEqual(reference, expected);
        }

        [TestMethod]
        public void TestTableReference()
        {
            var ctx = new Machine().Initialize();
            ctx.AddTable(new TableMeta() { Name = "table" });

            var input = "table";

            var parser = new Parser();
            parser.AddSource(new ColumnReference() { Table = "table" });
            var reference = parser.ParseReference(input, 0, ctx, new Parser.ReferenceFlags() { TableOnly = true });
            var expected = new ColumnReference() { Table = "table" };

            Assert.AreEqual(reference, expected);
        }

        [TestMethod]
        public void TestNumber()
        {
            var input = " 3.14 ";

            var next = new Parser().NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.LiteralNumber);
            Assert.AreEqual(next.NumberValue, 3.14);

            input = " 5";

            next = new Parser().NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.LiteralNumber);
            Assert.AreEqual(next.NumberValue, 5);

            input = " 4.99e5";

            next = new Parser().NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.LiteralNumber);
            Assert.AreEqual(next.NumberValue, 499000);
        }

        [TestMethod]
        public void TestKeyword()
        {
            var input = " WHERE ";

            var next = new Parser().NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.Keyword);
            Assert.AreEqual(next.KeywordValue, Keyword.Where);
        }

        [TestMethod]
        public void TestPonctuation()
        {
            var input = " , ";

            var next = new Parser().NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.Comma);
        }

        [TestMethod]
        public void TestSimpleExpression()
        {
            var input = "2+2";

            var expr = new Parser().ParseExpression(input, 0, new Context(), new Parser.ExpressionFlags(), true, false);

            var expected = new Expression()
            {
                FullText = "2+2",
                Identifier = null,
                Type = Expression.ExpressionType.Number,
                Nodes = new List<Expression.Node>
                {
                    new Expression.Node() { Kind = Expression.NodeKind.Number, NumberValue = 2 },
                    new Expression.Node() { Kind = Expression.NodeKind.Operator, OperatorValue = Operator.Plus },
                    new Expression.Node() { Kind = Expression.NodeKind.Number, NumberValue = 2 },
                }
            };

            Assert.AreEqual(expr, expected);
        }

        [TestMethod]
        public void TestBasicSelect()
        {
            var input = "select * from table";

            var ctx = new Context();
            ctx.AddTable(new TableMeta() { Name = "table" });

            var command = new Parser().ParseStatement(input, ctx);
            Assert.IsTrue(command is SelectStatement);

            var expected = new SelectStatement()
            {
                MainSource = new ColumnReference() { Table = "table" }
            };

            expected.OutputColumns.Add(new Expression()
            {
                Type = Expression.ExpressionType.Unknown,
                FullText = "*",
                Nodes = new List<Expression.Node>
                {
                    new Expression.Node() { Kind = Expression.NodeKind.Operator, OperatorValue = Operator.Asterisk }
                }
            });

            Assert.AreEqual(command, expected);
        }

        [TestMethod]
        public void TestMoreCompleteSelect()
        {
            var input = "select * from table where 1=1 order by a desc";

            var ctx = new Context();
            var column = new ColumnMeta() { Name = "a", Type = ColumnType.Number };
            ctx.Tables.Add(new TableMeta() { Name = "table", Columns = new List<ColumnMeta>() { column } });

            var command = new Parser().ParseStatement(input, ctx);
            Assert.IsTrue(command is SelectStatement);

            var expected = new SelectStatement()
            {
                MainSource = new ColumnReference() { Table = "table" },
                OutputOrder = new Ordering()
                {
                    Kind = OrderingKind.Descending,
                    OrderExpression = new Expression()
                    {
                        Type = Expression.ExpressionType.Number,
                        FullText = "a",
                        Nodes = new List<Expression.Node>()
                        {
                            new Expression.Node()
                            {
                                Kind = Expression.NodeKind.Reference,
                                ReferenceValue = new ColumnReference() { Column = "a" }
                            }
                        }
                    }
                },
                FilterConditions = new Expression()
                {
                    Type = Expression.ExpressionType.Number,
                    FullText = "1=1",
                    IsBoolean = true,
                    Nodes = new List<Expression.Node>()
                    {
                        new Expression.Node() { Kind = Expression.NodeKind.Number, NumberValue = 1 },
                        new Expression.Node() { Kind = Expression.NodeKind.Operator, OperatorValue = Operator.Equal },
                        new Expression.Node() { Kind = Expression.NodeKind.Number, NumberValue = 1 },
                    }
                }
            };

            expected.OutputColumns.Add(new Expression()
            {
                Type = Expression.ExpressionType.Unknown,
                FullText = "*",
                Nodes = new List<Expression.Node>
                {
                    new Expression.Node() { Kind = Expression.NodeKind.Operator, OperatorValue = Operator.Asterisk }
                }
            });

            Assert.AreEqual(command, expected);
        }

        [TestMethod]
        public void TestSelectWithFunctionAndColumn()
        {
            var input = "select CURRENT_DATE(), a, table.b from table";

            var ctx = new Machine().Initialize();
            var table = new TableMeta() { Name = "table" };
            ctx.AddColumn(new ColumnMeta() { Name = "a", Type = ColumnType.Number }, table);
            ctx.AddColumn(new ColumnMeta() { Name = "b", Type = ColumnType.String }, table);
            ctx.AddTable(table);

            var command = new Parser().ParseStatement(input, ctx);
            Assert.IsTrue(command is SelectStatement);

            var expected = new SelectStatement()
            {
                MainSource = new ColumnReference() { Table = "table", Identifier = "table", InputLength = 6 },
                OriginalText = input
            };

            expected.OutputColumns.Add(new Expression()
            {
                Type = Expression.ExpressionType.Date,
                FullText = "CURRENT_DATE()",
                Identifier = "CURRENT_DATE()",
                Nodes = new List<Expression.Node>
                {
                    new Expression.Node() { Kind = Expression.NodeKind.Function, FunctionCall = new FunctionCall() { Name = "CURRENT_DATE", Arguments = new List<Expression>() } }
                }
            });

            expected.OutputColumns.Add(new Expression()
            {
                Type = Expression.ExpressionType.Number,
                FullText = "a",
                Identifier = "a",
                Nodes = new List<Expression.Node>
                {
                    new Expression.Node() { Kind = Expression.NodeKind.Reference, ReferenceValue = new ColumnReference() { Column = "a", Table = "table" } }
                }
            });

            expected.OutputColumns.Add(new Expression()
            {
                Type = Expression.ExpressionType.String,
                FullText = "table.b",
                Identifier = "table.b",
                Nodes = new List<Expression.Node>
                {
                    new Expression.Node() { Kind = Expression.NodeKind.Reference, ReferenceValue = new ColumnReference() { Column = "b", Table = "table" } }
                }
            });

            Assert.AreEqual(command, expected);
        }

        [TestMethod]
        public void TestNullExpression()
        {
            var ctx = new Machine().Initialize();
            var table = new TableMeta() { Name = "table" };
            ctx.AddColumn(new ColumnMeta() { Name = "a", Type = ColumnType.Number }, table);
            ctx.AddTable(table);

            var input = "NULL";

            var result = new Parser().ParseExpression(input, 0, ctx, new Parser.ExpressionFlags(), true, false);

            Assert.AreEqual(result.Type, Expression.ExpressionType.Unknown);
            Assert.IsTrue(result.Nodes.Count == 1);
            Assert.AreEqual(result.Nodes[0].Kind, Expression.NodeKind.Null);
        }

        [TestMethod]
        public void TestNamedExpression()
        {
            var input = "table.a as name FROM";

            var ctx = new Machine().Initialize();
            var table = new TableMeta() { Name = "table" };
            ctx.AddColumn(new ColumnMeta() { Name = "a", Type = ColumnType.Number }, table);
            ctx.AddTable(table);

            var parser = new Parser();
            parser.AddSource(new ColumnReference() { Table = "table" });
            var result = parser.ParseExpression(input, 0, ctx, new Parser.ExpressionFlags() { IdentifierAllowed = true }, true, false);

            Assert.IsNotNull(result.Identifier);
            Assert.AreEqual(result.Identifier, "name");
        }

        [TestMethod]
        public void TestReferenceOnlyExpression()
        {
            var input = "table.a";

            var ctx = new Machine().Initialize();
            var table = new TableMeta() { Name = "table" };
            ctx.AddColumn(new ColumnMeta() { Name = "a", Type = ColumnType.Number }, table);
            ctx.AddTable(table);

            var parser = new Parser();
            parser.AddSource(new ColumnReference() { Table = "table" });
            var result = parser.ParseExpression(input, 0, ctx, new Parser.ExpressionFlags(), true, false);

            Assert.IsTrue(result.IsOnlyReference());
        }

        [TestMethod]
        public void TestFunctionCall()
        {
            var input = "CURRENT_DATE()";

            var ctx = new Machine().Initialize();

            var result = new Parser().ParseExpression(input, 0, ctx, new Parser.ExpressionFlags(), true, false);
            var expected = new Expression()
            {
                FullText = input,
                Identifier = input,
                IsBoolean = false,
                Type = Expression.ExpressionType.Date,
                Nodes = new List<Expression.Node>()
                {
                    new Expression.Node()
                    {
                        Kind = Expression.NodeKind.Function,
                        FunctionCall = new FunctionCall()
                        {
                            Name = "CURRENT_DATE",
                            Arguments = new List<Expression>()
                        }
                    }
                }
            };

            Assert.AreEqual(result, expected);
        }
    }
}
