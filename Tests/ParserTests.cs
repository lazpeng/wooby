using Microsoft.VisualStudio.TestTools.UnitTesting;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using wooby;

namespace Tests
{
    [TestClass]
    public class ParserTests
    {
        [TestMethod]
        public void TestOperatorParsing()
        {
            var input = "   +   ";

            var next = Parser.NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.Operator);
            Assert.AreEqual(next.OperatorValue, Operator.Plus);
        }

        [TestMethod]
        public void TestTokenDetection()
        {
            var input = " 'testing' SELECT 2.45 symbol";

            var next = Parser.NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.LiteralString);

            int offset = next.InputLength;

            next = Parser.NextToken(input, offset);
            Assert.AreEqual(next.Kind, Parser.TokenKind.Keyword);
            offset += next.InputLength;

            next = Parser.NextToken(input, offset);
            Assert.AreEqual(next.Kind, Parser.TokenKind.LiteralNumber);
            offset += next.InputLength;

            next = Parser.NextToken(input, offset);
            Assert.AreEqual(next.Kind, Parser.TokenKind.Symbol);
        }

        [TestMethod]
        public void TestString()
        {
            var input = " 'test string' ";

            var next = Parser.NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.LiteralString);
            Assert.AreEqual(next.StringValue, "test string");
        }

        [TestMethod]
        public void TestSymbol()
        {
            var input = " test another ";

            var next = Parser.NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.Symbol);
            Assert.AreEqual(next.StringValue, "test");
        }

        [TestMethod]
        public void TestNumber()
        {
            var input = " 3.14 ";

            var next = Parser.NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.LiteralNumber);
            Assert.AreEqual(next.NumberValue, 3.14);

            input = " 5";

            next = Parser.NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.LiteralNumber);
            Assert.AreEqual(next.NumberValue, 5);

            input = " 4.99e5";

            next = Parser.NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.LiteralNumber);
            Assert.AreEqual(next.NumberValue, 499000);
        }

        [TestMethod]
        public void TestKeyword()
        {
            var input = " WHERE ";

            var next = Parser.NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.Keyword);
            Assert.AreEqual(next.KeywordValue, Keyword.Where);
        }

        [TestMethod]
        public void TestPonctuation()
        {
            var input = " , ";

            var next = Parser.NextToken(input, 0);
            Assert.AreEqual(next.Kind, Parser.TokenKind.Comma);
        }

        [TestMethod]
        public void TestSimpleExpression()
        {
            var input = "2+2";

            var expr = Parser.ParseExpression(input, 0, new Context(), new Parser.ExpressionFlags());

            var expected = new Expression()
            {
                FullText = "2+2",
                Identifier = null,
                Type = Expression.ExpressionType.Number,
                Nodes = new List<Expression.Node>
                {
                    new Expression.Node() { Kind = Expression.NodeKind.Number, NumberValue = 2},
                    new Expression.Node() { Kind = Expression.NodeKind.Operator, OperatorValue = Operator.Plus },
                    new Expression.Node() { Kind = Expression.NodeKind.Number, NumberValue = 2},
                }
            };

            Assert.AreEqual(expr, expected);
        }

        [TestMethod]
        public void TestBasicSelect()
        {
            var input = "select * from table";

            var ctx = new Context();
            ctx.Schemas[0].Tables.Add(new TableMeta() { Name = "table" });

            var command = Parser.ParseCommand(input, ctx);
            Assert.IsTrue(command is SelectCommand);

            var expected = new SelectCommand()
            {
                MainSource = new TableReference() { Table = "table" }
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
    }
}
