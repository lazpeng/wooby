﻿using Microsoft.VisualStudio.TestTools.UnitTesting;

using System;
using System.Collections.Generic;
using System.Linq;

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
        public void TestTableReference()
        {
            var ctx = new Machine().Initialize();
            ctx.AddTable(new TableMeta() { Name = "t" });

            var input = "t";

            var parser = new Parser();
            var reference = parser.ParseReference(input, 0, ctx, new SelectStatement(), new Parser.ReferenceFlags() { TableOnly = true });
            var expected = new ColumnReference() { Table = "t" };

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

            var expr = new Parser().ParseExpression(input, 0, new Context(), new SelectStatement(), new Parser.ExpressionFlags(), true, false);

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
            var input = "select * from t";

            var ctx = new Context();
            ctx.AddTable(new TableMeta() { Name = "t" });

            var command = new Parser().ParseStatement(input, ctx);
            Assert.IsTrue(command is SelectStatement);

            var expected = new SelectStatement()
            {
                MainSource = new ColumnReference() { Table = "t" }
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
            var input = "select * from t where 1=1 order by a desc";

            var ctx = new Context();
            var column = new ColumnMeta() { Name = "a", Type = ColumnType.Number };
            ctx.Tables.Add(new TableMeta() { Name = "t", Columns = new List<ColumnMeta>() { column } });

            var command = new Parser().ParseStatement(input, ctx);
            Assert.IsTrue(command is SelectStatement);

            var expected = new SelectStatement()
            {
                MainSource = new ColumnReference() { Table = "t" },
                OutputOrder = new List<Ordering>()
                {
                    new Ordering()
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
        [ExpectedException(typeof(Exception))]
        public void FailWithInvalidTableName()
        {
            // Uses reserved keyword TABLE
            var input = "select CURRENT_DATE(), a, table.b from table";
            var ctx = new Machine().Initialize();
            var table = new TableMeta() { Name = "t" }
            .AddColumn("a", ColumnType.Number)
            .AddColumn("b", ColumnType.String);
            ctx.AddTable(table);

            new Parser().ParseStatement(input, ctx);
            Assert.Fail("Did not raise parser exception");
        }

        [TestMethod]
        public void TestSelectWithFunctionAndColumn()
        {
            var input = "select CURRENT_DATE(), a, t.b from t";

            var ctx = new Machine().Initialize();
            var table = new TableMeta() { Name = "t" }
            .AddColumn("a", ColumnType.Number)
            .AddColumn("b", ColumnType.String);
            ctx.AddTable(table);

            var command = new Parser().ParseStatement(input, ctx);
            Assert.IsTrue(command is SelectStatement);

            var expected = new SelectStatement()
            {
                MainSource = new ColumnReference() { Table = "t", Identifier = "t", InputLength = 6 },
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
                    new Expression.Node() { Kind = Expression.NodeKind.Reference, ReferenceValue = new ColumnReference() { Column = "a", Table = "t" } }
                }
            });

            expected.OutputColumns.Add(new Expression()
            {
                Type = Expression.ExpressionType.String,
                FullText = "t.b",
                Identifier = "t.b",
                Nodes = new List<Expression.Node>
                {
                    new Expression.Node() { Kind = Expression.NodeKind.Reference, ReferenceValue = new ColumnReference() { Column = "b", Table = "t" } }
                }
            });

            Assert.AreEqual(command, expected);
        }

        [TestMethod]
        public void TestNullExpression()
        {
            var ctx = new Machine().Initialize();

            var input = "NULL";

            var result = new Parser().ParseExpression(input, 0, ctx, new SelectStatement(), new Parser.ExpressionFlags(), true, false);

            Assert.AreEqual(result.Type, Expression.ExpressionType.Unknown);
            Assert.IsTrue(result.Nodes.Count == 1);
            Assert.AreEqual(result.Nodes[0].Kind, Expression.NodeKind.Null);
        }

        [TestMethod]
        public void TestNamedExpression()
        {
            var input = "select t.a as name FROM t";

            var ctx = new Machine().Initialize();
            var table = new TableMeta() { Name = "t" }.AddColumn("a", ColumnType.Number);
            ctx.AddTable(table);

            SelectStatement statement = (SelectStatement) new Parser().ParseStatement(input, ctx);
            var result = statement.OutputColumns[0];

            Assert.IsNotNull(result.Identifier);
            Assert.AreEqual(result.Identifier, "name");
        }

        [TestMethod]
        public void TestReferenceOnlyExpression()
        {
            var input = "SELECT t.a FROM t";

            var ctx = new Machine().Initialize();
            var table = new TableMeta() { Name = "t" }.AddColumn("a", ColumnType.Number);
            ctx.AddTable(table);

            SelectStatement statement = (SelectStatement)new Parser().ParseStatement(input, ctx);
            var result = statement.OutputColumns[0];

            Assert.IsTrue(result.IsOnlyReference());
        }

        [TestMethod]
        public void TestFunctionCall()
        {
            var input = "CURRENT_DATE()";

            var ctx = new Machine().Initialize();

            var result = new Parser().ParseExpression(input, 0, ctx, new SelectStatement(), new Parser.ExpressionFlags(), true, false);
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

        [TestMethod]
        public void TestOrderByMultipleClauses()
        {
            var input = "SELECT * FROM a ORDER BY b desc, c";

            var ctx = new Machine().Initialize();
            var table = new TableMeta() { Name = "a" }
            .AddColumn("b", ColumnType.Number)
            .AddColumn("c", ColumnType.Number);
            ctx.AddTable(table);

            var result = new Parser().ParseSelect(input, 0, ctx, new Parser.StatementFlags(), null);

            var expected = new List<Ordering>()
            {
                new Ordering()
                {
                    Kind = OrderingKind.Descending,
                    OrderExpression = new Expression()
                    {
                        FullText = " b",
                        Identifier = "a.b",
                        IsBoolean = false,
                        Type = Expression.ExpressionType.Number,
                        Nodes = new List<Expression.Node>()
                        {
                            new Expression.Node()
                            {
                                Kind = Expression.NodeKind.Reference,
                                ReferenceValue = new ColumnReference()
                                {
                                    Column = "b",
                                    Table = "a"
                                }
                            }
                        }
                    }
                },
                new Ordering()
                {
                    Kind = OrderingKind.Ascending,
                    OrderExpression = new Expression()
                    {
                        FullText = " c",
                        Identifier = "a.c",
                        IsBoolean = false,
                        Type = Expression.ExpressionType.Number,
                        Nodes = new List<Expression.Node>()
                        {
                            new Expression.Node()
                            {
                                Kind = Expression.NodeKind.Reference,
                                ReferenceValue = new ColumnReference()
                                {
                                    Column = "c",
                                    Table = "a"
                                }
                            }
                        }
                    }
                }
            };

            Assert.IsTrue(result.OutputOrder.SequenceEqual(expected));
        }
    }
}
